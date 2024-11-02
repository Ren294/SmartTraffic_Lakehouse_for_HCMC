from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def create_spark_session():
    return SparkSession.builder \
        .appName("WeatherHCMC-Kafka-to-Hudi") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar") \
        .config("spark.jars.packages",
                "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "ren294") \
        .config("spark.hadoop.fs.s3a.secret.key", "trungnghia294") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
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


def process_weather_stream():
    spark = create_spark_session()
    schema_ddl = get_schema_ddl()

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "weatherHCMC_out") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse CSV value
    parsed_df = df.select(from_csv(col("value").cast("string"), schema_ddl).alias("data")) \
        .select("data.*") \
        .withColumn("record_id", concat(col("date"), lit("_"), col("datetime"))) \
        .withColumn("timestamp", to_timestamp(concat(col("date"), lit(" "), col("datetime")), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("year_month", date_format(col("timestamp"), "yyyy-MM"))

    # Hudi options
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

    def write_batch(batch_df, batch_id):
        if not batch_df.rdd.isEmpty():
            batch_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save("s3a://silver/weather_hcmc")

    # Write stream
    query = parsed_df.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", "file:///Users/ren/Downloads/Data/spark-warehouse") \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_weather_stream()
