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
    """Get schema definition for traffic data"""
    return StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("license_number", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("speed_kmph", DecimalType(10, 2), True),
        StructField("street", StringType(), True),
        StructField("district", StringType(), True),
        StructField("city", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("length_meters", DecimalType(10, 2), True),
        StructField("width_meters", DecimalType(10, 2), True),
        StructField("height_meters", DecimalType(10, 2), True),
        StructField("vehicle_type", StringType(), True),
        StructField("vehicle_classification", StringType(), True),
        StructField("latitude", DecimalType(10, 2), True),
        StructField("longitude", DecimalType(10, 2), True),
        StructField("is_running", BooleanType(), True),
        StructField("rpm", IntegerType(), True),
        StructField("oil_pressure", StringType(), True),
        StructField("fuel_level_percentage", IntegerType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("internal_temperature_celsius", DecimalType(10, 2), True),
        StructField("destination_street", StringType(), True),
        StructField("destination_district", StringType(), True),
        StructField("destination_city", StringType(), True),
        StructField("eta", TimestampType(), True),
        # Adding date field for partitioning
        StructField("date", StringType(), True)
    ])


def get_hudi_options(table_name: str) -> Dict[str, str]:
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'vehicle_id,timestamp,street',
        'hoodie.datasource.write.partitionpath.field': 'date',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'timestamp',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }


def get_validation_conditions():
    """Return a list of validation conditions using Spark SQL expressions"""
    return [
        # Basic presence checks
        col('vehicle_id').isNotNull(),
        col('timestamp').isNotNull(),
        col('street').isNotNull(),
        col('district').isNotNull(),

        # Numerical range validations
        (col('speed_kmph').isNull() | (col('speed_kmph') >= 0)),
        (col('length_meters').isNull() | (col('length_meters') > 0)),
        (col('width_meters').isNull() | (col('width_meters') > 0)),
        (col('height_meters').isNull() | (col('height_meters') > 0)),

        # Additional checks
        (col('fuel_level_percentage').isNull() |
         ((col('fuel_level_percentage') >= 0) & (col('fuel_level_percentage') <= 100))),

        (col('passenger_count').isNull() | (col('passenger_count') >= 0)),
        (col('rpm').isNull() | (col('rpm') >= 0)),

        # Timestamp validation
        col('eta').isNotNull() &
        (col('eta') >= col('timestamp')),

        # Geolocation validation
        (col('latitude').isNull() |
         ((col('latitude') >= -90) & (col('latitude') <= 90))),
        (col('longitude').isNull() |
         ((col('longitude') >= -180) & (col('longitude') <= 180)))
    ]


def calculate_aggregations(df):
    """Calculate aggregations for metadata"""
    aggs = df.agg(
        count("*").alias("records_count"),
        avg("speed_kmph").alias("average_speed"),
        avg("passenger_count").alias("average_passengers"),
        sum("fuel_level_percentage").alias("total_fuel_percentage")
    ).collect()[0]

    return {
        "records_count": str(aggs["records_count"]),
        "average_speed": "{:.2f}".format(float(aggs["average_speed"])),
        "average_passengers": "{:.2f}".format(float(aggs["average_passengers"])),
        "total_fuel_percentage": "{:.2f}".format(float(aggs["total_fuel_percentage"]))
    }


def process_batch(df, epoch_id, spark_session):
    """Process each batch of traffic data"""
    try:
        client = get_lakefs_client()
        redis_client = get_redis_client()
        repo = Repository("silver", client=client)

        # Add date column for partitioning
        df = df.withColumn("date", date_format(
            col("timestamp"), "yyyy-MM-dd"))

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
                .option("topic", "traffic_dead_letter_queue") \
                .save()

        # Process valid records by date
        for date_group in valid_records.groupBy("date").agg(
            collect_list(struct([col(c) for c in valid_records.columns])).alias(
                "records")
        ).collect():
            date = date_group["date"]
            records = date_group["records"]

            branch_name = f"branch_traffic_{date}"

            # Check if branch exists
            branch_exists = False
            for branch in repo.branches():
                if branch.id == branch_name:
                    print(f"Branch '{branch_name}' already exists.")
                    branch_exists = True
                    break

            # Create new branch if it doesn't exist
            if not branch_exists:
                traffic_branch = repo.branch(branch_name).create(
                    source_reference="staging_traffic")
                print("New branch created:", branch_name,
                      "with commit ID:", traffic_branch.get_commit().id)
            else:
                traffic_branch = repo.branch(branch_name)

            # Create DataFrame from records and calculate aggregations
            records_df = spark_session.createDataFrame(
                records, schema=get_schema())
            aggs = calculate_aggregations(records_df)

            # Write to Hudi table
            records_df.write \
                .format("hudi") \
                .options(**get_hudi_options(f"traffic_HCMC")) \
                .mode("append") \
                .save(f"s3a://silver/{branch_name}/traffic/")

            # Prepare metadata
            metadata = {
                "date": date,
                "records_count": aggs["records_count"],
                "average_speed": aggs["average_speed"],
                "average_passengers": aggs["average_passengers"],
                "total_fuel_percentage": aggs["total_fuel_percentage"]
            }

            try:
                commit_response = traffic_branch.commit(
                    message=f"Added traffic data for {date}",
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
                    "silver_merge_queue_traffic", json.dumps(md))

            except Exception as commit_error:
                print(f"Error committing to branch \
                  {branch_name}: {str(commit_error)}")
                raise commit_error

    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")
        raise e


def process_traffic_stream():
    # Create Spark session
    spark = create_spark_session(
        lakefs_user["username"], lakefs_user["password"])
    schema = get_schema()

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "trafficHCMC_out") \
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

    checkpoint_location = "file:///opt/spark-data/checkpoint_traffic_silver"

    # Process the stream
    query = parsed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, spark)) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_traffic_stream()
