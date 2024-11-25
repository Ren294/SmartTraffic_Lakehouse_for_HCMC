from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from typing import Dict
from lakefs import Repository
from common import get_redis_client, get_lakefs_client, get_lakefs, create_spark_session

lakefs_user = get_lakefs()


def get_schema():
    """Get schema definition for parkinglot data"""
    return StructType([
        StructField("parkinglotid", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("location", StringType(), False),
        StructField("totalspaces", IntegerType(), False),
        StructField("availablespaces", IntegerType(), False),
        StructField("carspaces", IntegerType(), False),
        StructField("motorbikespaces", IntegerType(), False),
        StructField("bicyclespaces", IntegerType(), False),
        StructField("type", StringType(), False),
        StructField("hourlyrate", DecimalType(10, 2), False),
        StructField("last_update", TimestampType(), True)
    ])


def get_hudi_options(table_name: str) -> Dict[str, str]:
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'parkinglotid',
        'hoodie.datasource.write.precombine.field': 'last_update',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }


def process_batch(df, epoch_id, spark_session):
    """Process each batch of parkinglot data"""
    try:
        client = get_lakefs_client()
        redis_client = get_redis_client()
        repo = Repository("silver", client=client)

        # Extract relevant data from Debezium JSON
        parsed_df = df.select(
            from_json(col("value").cast("string"), "struct<after:struct<parkinglotid:int,name:string,location:string,totalspaces:int,availablespaces:int,carspaces:int,motorbikespaces:int,bicyclespaces:int,type:string,hourlyrate:decimal(10,2)>>").alias("parsed_value")
        ).select(
            col("parsed_value.after.*"),
            current_timestamp().alias("last_update")
        )

        # Generate branch name based on current timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        branch_name = f"parking_{timestamp}_modified"

        # Check if branch exists
        branch_exists = False
        for branch in repo.branches():
            if branch.id == branch_name:
                branch_exists = True
                break

        if not branch_exists:
            parking_branch = repo.branch(branch_name).create(
                source_reference="staging_parking")
            print("New branch created:", branch_name,
                  "with commit ID:", parking_branch.get_commit().id)
        else:
            parking_branch = repo.branch(branch_name)

        # Write to Hudi table
        if parsed_df.count() > 0:
            parsed_df.write \
                .format("hudi") \
                .options(**get_hudi_options("parking_parkinglot")) \
                .mode("append") \
                .save(f"s3a://silver/{branch_name}/parking/parkinglot/")

            # Add to Redis for processing
            config = {
                'table_name': 'parkinglot',
                'modified_branch': branch_name,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            redis_client.rpush(
                "parking_modified_parkinglot", json.dumps(config))

    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")
        raise e


def process_parkinglot_stream():
    """Main function to process parkinglot stream from Kafka"""
    spark = create_spark_session(
        lakefs_user["username"],
        lakefs_user["password"],
        "ParkinglotToSilverLayer"
    )

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "parking.parkinglot") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Process the stream
    checkpoint_location = "file:///opt/spark-data/checkpoint_parkinglot_silver"

    query = df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, spark)) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_parkinglot_stream()
