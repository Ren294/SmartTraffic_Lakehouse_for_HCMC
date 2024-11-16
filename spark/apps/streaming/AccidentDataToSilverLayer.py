import json
from datetime import datetime
from typing import Dict, List, Any
from functools import reduce
from connection import get_redis_client, get_lakefs_client, get_lakefs
from config import create_spark_session
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
        (col('accident_severity').isNull() | (col('accident_severity') >= 1)
         & (col('accident_severity') <= 10)),
        (col('number_of_vehicles').isNull() | col('number_of_vehicles') >= 0),
        (col('congestion_km').isNull() | col('congestion_km') >= 0),

        # Vehicle involvement checks
        (col('car_involved').isNull() | col('car_involved') >= 0),
        (col('motobike_involved').isNull() | col('motobike_involved') >= 0),
        (col('other_involved').isNull() | col('other_involved') >= 0),

        # Timestamp validation
        col('estimated_recovery_time').isNotNull() &
        (col('estimated_recovery_time') >= col('accident_time'))
    ]


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
            collect_list(struct([col(c) for c in df.columns])).alias("records")
        ).collect():
            date = date_group["date"]
            records = date_group["records"]

            branch_name = f"branch_accidents_{date}"

            branch_exists = False

            # Check if branch exists
            for branch in repo.branches():
                if branch.id == branch_name:
                    print(f"Branch '{branch_name}' already exists.")
                    branch_exists = True
                    break

            # Create new branch if it doesn't exist
            if not branch_exists:
                accidents_branch = repo.branch(
                    branch_name).create(source_reference="staging_accidents")
                print("New branch created:", branch_name,
                      "with commit ID:", accidents_branch.get_commit().id)
            else:
                accidents_branch = repo.branch(branch_name)

            # Process records and write to Hudi
            schema = get_schema()
            rows = []
            for record in records:
                row = []
                for field in schema.fields:
                    value = record[field.name]
                    if value is None:
                        row.append(None)
                    else:
                        try:
                            if isinstance(field.dataType, StringType):
                                row.append(str(value))
                            elif isinstance(field.dataType, IntegerType):
                                row.append(int(value) if value != '' else None)
                            elif isinstance(field.dataType, DoubleType):
                                row.append(
                                    float(value) if value != '' else None)
                            elif isinstance(field.dataType, TimestampType):
                                # Already in timestamp format
                                row.append(value)
                            else:
                                row.append(value)
                        except (ValueError, TypeError):
                            row.append(None)
                rows.append(row)

            records_df = spark_session.createDataFrame(rows, schema)

            # Write to Hudi table
            records_df.write \
                .format("hudi") \
                .options(**get_hudi_options(f"accidents_HCMC")) \
                .mode("append") \
                .save(f"s3a://silver/{branch_name}/accidents/")

            # Prepare metadata
            metadata = {
                "date": date,
                "records_count": str(len(records)),
                "total_accidents": str(len(records)),
                "average_severity": str(round(sum(r["accident_severity"] for r in records) / len(records), 2)),
                "total_congestion_km": str(round(sum(r["congestion_km"] for r in records), 2))
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
                redis_client.rpush("silver_merge_queue_accidents",
                                   json.dumps(md))

            except Exception as commit_error:
                print(f"Error committing to branch {
                      branch_name}: {str(commit_error)}")
                raise commit_error

    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")
        raise e


def process_accidents_stream():
    # Create Spark session
    spark = create_spark_session(
        lakefs_user["username"], lakefs_user["password"])
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
