from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType
from pyspark.sql import DataFrame


def read_silver_main(spark, path_table):
    s3_path = f"s3a://silver/main/{path_table}"
    df = spark.read.format("hudi").load(s3_path)
    return df


def write_to_warehouse(df, table_name, table_path, operation="upsert", recordkey="record_id", precombine="timestamp"):
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": recordkey,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.precombine.field": precombine,
        # "hoodie.datasource.write.partitionpath.field": partitionpath,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://hive-metastore:9083",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": table_name,
    }

    s3_path = f"{table_path}"

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(s3_path)


def read_warehouse(spark, path_table):
    s3_path = f"s3a://gold/main/warehouse/{path_table}"
    df = spark.read.format("hudi").load(s3_path)
    return df
