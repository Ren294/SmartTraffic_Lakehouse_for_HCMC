"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
import argparse
from pyspark.sql import SparkSession
from common import get_redis_client, get_lakefs_client, get_lakefs, create_spark_session
import json

lakefs_user = get_lakefs()


def get_hudi_options(table_name: str) -> dict:
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


def merge_data(spark, source_branch, target_branch, date):
    # Read source data
    source_df = spark.read.format("hudi") \
        .load(f"s3a://silver/{source_branch}/traffic/") \
        .where(f"date = '{date}'")

    # Write to target branch
    source_df.write \
        .format("hudi") \
        .options(**get_hudi_options("traffic_HCMC")) \
        .mode("append") \
        .save(f"s3a://silver/{target_branch}/traffic/")


if __name__ == "__main__":
    redis_client = get_redis_client()
    merge_request_str = redis_client.lpop("silver_merge_queue_traffic_checked")
    merge_request = json.loads(merge_request_str)

    spark = create_spark_session(
        lakefs_user["username"], lakefs_user["password"], "SilverTrafficStagingMerge")
    merge_data(
        spark,
        merge_request["source_branch"],
        merge_request["target_branch"],
        merge_request["date"]
    )
    spark.stop()
