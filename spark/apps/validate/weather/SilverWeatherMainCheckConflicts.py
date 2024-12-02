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


def check_conflicts(spark, source_branch, target_branch, date):
    source_df = spark.read.format("hudi")\
        .load(f"s3a://silver/{source_branch}/weather/")\
        .where(f"date = '{date}'")

    target_df = None
    try:
        target_df = spark.read.format("hudi")\
            .load(f"s3a://silver/{target_branch}/weather/")\
            .where(f"date = '{date}'")
    except:
        print("No data found in main branch. Proceeding without conflict check.")

    if target_df and target_df.count() > 0:
        target_count = target_df.count()

        if target_count != 24:
            raise Exception(f"Data integrity error: Target has \
              {target_count} records. Expected 24.")

        conflicts = source_df.join(
            target_df,
            ["datetime", "date"],
            "inner"
        )

        if conflicts.count() > 0:
            raise Exception("Conflicts detected in the data")
    spark.stop()

    config = {
        'source_branch': source_branch,
        'target_branch': target_branch,
        'date': date
    }
    redis_client = get_redis_client()
    redis_client.rpush("main_merge_queue_weather_checked",
                       json.dumps(config))


if __name__ == "__main__":
    redis_client = get_redis_client()
    merge_request_str = redis_client.lpop(
        "main_merge_queue_weather_pre_checked")
    merge_request = json.loads(merge_request_str)

    spark = create_spark_session(
        lakefs_user["username"], lakefs_user["password"], "SilverWeatherMainCheckConflicts")
    check_conflicts(
        spark, merge_request["source_branch"], merge_request["target_branch"], merge_request["date"])
    spark.stop()
