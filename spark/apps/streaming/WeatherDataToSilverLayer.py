from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lakefs import Client
import json
import requests
from datetime import datetime


def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherHCMC-Kafka-to-LakeFS") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .config("spark.jars.packages",
                "org.apache.hudi:hudi-spark3.2-bundle_2.12:0.15.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "ren294") \
        .config("spark.hadoop.fs.s3a.secret.key", "trungnghia294") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.lakefs.impl", "io.lakefs.LakeFSFileSystem") \
        .config("spark.hadoop.fs.lakefs.access.key", "AKIAJC5AQUW4OXQYCRAQ") \
        .config("spark.hadoop.fs.lakefs.secret.key", "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") \
        .config("spark.hadoop.fs.lakefs.endpoint", "http://lakefs:8000") \
        .getOrCreate()


def get_schema_ddl():
    return """
        datetime STRING,
        datetimeEpoch LONG,
        temp FLOAT,
        feelslike FLOAT,
        humidity FLOAT,
        dew FLOAT,
        precip FLOAT,
        precipprob FLOAT,
        snow FLOAT,
        snowdepth FLOAT,
        preciptype STRING,
        windgust FLOAT,
        windspeed FLOAT,
        winddir FLOAT,
        pressure FLOAT,
        visibility FLOAT,
        cloudcover FLOAT,
        solarradiation FLOAT,
        solarenergy FLOAT,
        uvindex INT,
        severerisk INT,
        conditions STRING,
        icon STRING,
        source STRING,
        timezone STRING,
        name STRING,
        latitude FLOAT,
        longitude FLOAT,
        resolvedAddress STRING,
        date STRING,
        address STRING,
        tzoffset INT,
        day_visibility FLOAT,
        day_cloudcover FLOAT,
        day_uvindex INT,
        day_description STRING,
        day_tempmin FLOAT,
        day_windspeed FLOAT,
        day_icon STRING,
        day_precip FLOAT,
        day_tempmax FLOAT,
        day_precipcover FLOAT,
        day_pressure FLOAT,
        day_preciptype STRING,
        day_humidity FLOAT,
        day_conditions STRING,
        day_feelslike FLOAT,
        day_dew FLOAT,
        day_sunrise STRING,
        day_sunriseEpoch LONG,
        day_feelslikemax FLOAT,
        day_windgust FLOAT,
        day_solarenergy FLOAT,
        day_sunset STRING,
        day_snowdepth FLOAT,
        day_sunsetEpoch LONG,
        day_severerisk INT,
        day_solarradiation FLOAT,
        day_precipprob FLOAT,
        day_temp FLOAT,
        day_winddir FLOAT,
        day_moonphase FLOAT,
        day_feelslikemin FLOAT,
        day_snow FLOAT
    """


def validate_data_types(df, schema):
    try:
        for field in schema.fields:
            if field.dataType == FloatType():
                df = df.withColumn(field.name, col(
                    field.name).cast(FloatType()))
            elif field.dataType == LongType():
                df = df.withColumn(field.name, col(
                    field.name).cast(LongType()))
            elif field.dataType == IntegerType():
                df = df.withColumn(field.name, col(
                    field.name).cast(IntegerType()))
        return df, True
    except Exception as e:
        return df, False


class LakeFSManager:
    def __init__(self, endpoint, access_key, secret_key):
        self.client = Client(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key
        )

    def check_and_create_branch(self, repository, source_branch, new_branch):
        try:
            # Check if branch exists
            self.client.branches.get(repository, new_branch)
            return True
        except:
            # Create new branch if it doesn't exist
            self.client.branches.create(
                repository=repository,
                name=new_branch,
                source=source_branch
            )
            return True


def process_weather_stream():
    spark = create_spark_session()
    schema = get_schema_ddl()
    lakefs_manager = LakeFSManager(
        endpoint="http://lakefs:8000",
        access_key="AKIAJC5AQUW4OXQYCRAQ",
        secret_key="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"
    )

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "weatherHCMC_out") \
        .option("startingOffsets", "earliest") \
        .load()

    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        # Parse and validate data
        parsed_df = batch_df.select(from_csv(col("value").cast("string"), schema).alias("data")) \
            .select("data.*")

        validated_df, is_valid = validate_data_types(parsed_df, schema)

        if not is_valid:
            # Write invalid records to dead letter queue
            invalid_records = validated_df.selectExpr("CAST(value AS STRING)") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("topic", "dead_queue_weather") \
                .save()
            return

        # Process valid records
        for row in validated_df.collect():
            date = row["date"]
            branch_name = f"branch_weather_{date}"

            # Check/create branch
            lakefs_manager.check_and_create_branch(
                repository="silver",
                source_branch="staging_weather",
                new_branch=branch_name
            )

            # Write to LakeFS
            hudi_options = {
                "hoodie.table.name": "weather_hcmc",
                "hoodie.datasource.write.recordkey.field": "record_id",
                "hoodie.datasource.write.precombine.field": "timestamp",
                "hoodie.datasource.write.partitionpath.field": "year_month",
                "hoodie.datasource.write.table.name": "weather_hcmc",
                "hoodie.datasource.write.operation": "upsert",
                "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
                "hoodie.datasource.write.commitmeta.key.prefix": "kafka"
            }

            validated_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(f"lakefs://silver/{branch_name}/weather_hcmc")

            # Commit changes
            lakefs_manager.client.commits.commit(
                repository="silver",
                branch=branch_name,
                message=f"Added weather data for {date}"
            )

    # Write stream
    query = df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "file:///opt/spark-data/checkpoint-weather-lakefs") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_weather_stream()
