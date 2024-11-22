import json
from datetime import datetime
from typing import Dict, List, Any
from functools import reduce
from common import get_redis_client, get_lakefs_client, get_lakefs, create_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lakefs import Repository, Client

lakefs_user = get_lakefs()


def get_schema():
    """Get schema definition for accident data"""
    return StructType([
        StructField("road_name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("city", StringType(), True),
        StructField("car_involved", IntegerType(), True),
        StructField("motobike_involved", IntegerType(), True),
        StructField("other_involved", IntegerType(), True),
        StructField("accident_severity", IntegerType(), True),
        StructField("accident_time", TimestampType(), True),
        StructField("number_of_vehicles", IntegerType(), True),
        StructField("estimated_recovery_time", TimestampType(), True),
        StructField("congestion_km", DoubleType(), True),
        StructField("description", StringType(), True),
        # Adding date field for partitioning
        StructField("date", StringType(), True)
    ])


def get_hudi_options(table_name: str) -> Dict[str, str]:
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'accident_time,road_name,district',
        'hoodie.datasource.write.partitionpath.field': 'date',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'accident_time',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }


def get_validation_conditions():
    """Return a list of validation conditions using Spark SQL expressions"""
    return [
        # Basic presence checks
        col('accident_time').isNotNull(),
        col('road_name').isNotNull(),
        col('district').isNotNull(),

        # Numerical range validations
        (col('accident_severity').isNull() |
         ((col('accident_severity') >= 1) & (col('accident_severity') <= 10))),

        # Fix: Correct syntax for number_of_vehicles validation
        (col('number_of_vehicles').isNull() |
         (col('number_of_vehicles') >= 0)),

        # Fix: Correct syntax for congestion_km validation
        (col('congestion_km').isNull() |
         (col('congestion_km') >= 0)),

        # Vehicle involvement checks
        (col('car_involved').isNull() |
         (col('car_involved') >= 0)),

        (col('motobike_involved').isNull() |
         (col('motobike_involved') >= 0)),

        (col('other_involved').isNull() |
         (col('other_involved') >= 0)),

        # Timestamp validation
        col('estimated_recovery_time').isNotNull() &
        (col('estimated_recovery_time') >= col('accident_time')),

        # Additional validation: Total vehicles check
        ((col('car_involved') + col('motobike_involved') + \
         col('other_involved')) == col('number_of_vehicles'))
    ]


def calculate_aggregations(df):
    """Calculate aggregations for metadata"""
    aggs = df.agg(
        count("*").alias("records_count"),
        avg("accident_severity").alias("average_severity"),
        sum("congestion_km").alias("total_congestion_km")
    ).collect()[0]

    return {
        "records_count": str(aggs["records_count"]),
        "average_severity": "{:.2f}".format(float(aggs["average_severity"])),
        "total_congestion_km": "{:.2f}".format(float(aggs["total_congestion_km"]))
    }


def process_batch(df, epoch_id, spark_session):
    """Process each batch of accident data"""
    try:
        client = get_lakefs_client()
        redis_client = get_redis_client()
        repo = Repository("silver", client=client)

        # Add date column for partitioning
        df = df.withColumn("date", date_format(
            col("accident_time"), "yyyy-MM-dd"))

        # Apply all validation conditions
        validation_conditions = get_validation_conditions()
        combined_condition = reduce(lambda x, y: x & y, validation_conditions)

        # Filter valid and invalid records
        valid_records = df.filter(combined_condition)
        invalid_records = df.filter(~combined_condition)

        # Handle invalid records
        if invalid_records.count() > 0:
            invalid_records.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("topic", "accidents_dead_letter_queue") \
                .save()

        # Process valid records by date
        for date_group in valid_records.groupBy("date").agg(
            collect_list(struct([col(c) for c in valid_records.columns])).alias(
                "records")
        ).collect():
            date = date_group["date"]
            records = date_group["records"]

            branch_name = f"branch_accidents_{date}"

            # Check if branch exists
            branch_exists = False
            for branch in repo.branches():
                if branch.id == branch_name:
                    print(f"Branch '{branch_name}' already exists.")
                    branch_exists = True
                    break

            # Create new branch if it doesn't exist
            if not branch_exists:
                accidents_branch = repo.branch(branch_name).create(
                    source_reference="staging_accidents")
                print("New branch created:", branch_name,
                      "with commit ID:", accidents_branch.get_commit().id)
            else:
                accidents_branch = repo.branch(branch_name)

            # Create DataFrame from records and calculate aggregations
            records_df = spark_session.createDataFrame(
                records, schema=get_schema())
            aggs = calculate_aggregations(records_df)

            # Write to Hudi table
            records_df.write \
                .format("hudi") \
                .options(**get_hudi_options(f"accidents_HCMC")) \
                .mode("append") \
                .save(f"s3a://silver/{branch_name}/accidents/")

            # Prepare metadata
            metadata = {
                "date": date,
                "records_count": aggs["records_count"],
                "average_severity": aggs["average_severity"],
                "total_congestion_km": aggs["total_congestion_km"]
            }

            try:
                commit_response = accidents_branch.commit(
                    message=f"Added accident data for {date}",
                    metadata=metadata
                )
                commit_id = commit_response.get_commit().id

                md = {
                    "branch": branch_name,
                    "date": date,
                    "commit_id": commit_id,
                    "metadata": json.dumps(metadata)
                }

                # Add to Redis merge queue
                redis_client.rpush(
                    "silver_merge_queue_accidents", json.dumps(md))

            except Exception as commit_error:
                print(f"Error committing to branch \
                  {branch_name}: {str(commit_error)}")
                raise commit_error

    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")
        raise e


def process_accidents_stream():
    # Create Spark session
    spark = create_spark_session(
        lakefs_user["username"], lakefs_user["password"], "AccidentDataToSilverLayer")
    schema = get_schema()

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "accidentsHCMC_out") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse CSV data from Kafka
    parsed_df = df.select(
        split(col("value").cast("string"), ",").alias("csv_columns")
    ).select(
        *[col("csv_columns").getItem(i).cast(schema[i].dataType).alias(schema[i].name)
          for i in range(len(schema) - 1)]  # -1 because 'date' is derived
    )

    checkpoint_location = "file:///opt/spark-data/checkpoint_accidents_silver"

    # Process the stream
    query = parsed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, spark)) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_accidents_stream()
