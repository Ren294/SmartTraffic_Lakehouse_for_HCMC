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
    # Read source data
    source_df = spark.read.format("hudi")\
        .load(f"s3a://silver/{source_branch}/accidents/")\
        .where(f"date = '{date}'")

    # Read target data
    target_df = None
    try:
        # Try to load the target data if the path exists
        target_df = spark.read.format("hudi")\
            .load(f"s3a://silver/{target_branch}/accidents/")\
            .where(f"date = '{date}'")
    except:
        print("No data found. Proceeding without conflict check.")

    # If target_df is not empty, proceed with conflict checks
    if target_df and target_df.count() > 0:
        # Compare records by unique key (accident_time, road_name, district) to detect conflicts
        conflicts = source_df.join(
            target_df,
            ["accident_time", "road_name", "district", "date"],
            "inner"
        )

        if conflicts.count() > 0:
            # Check if there are any actual differences in the data
            # Exclude timestamp fields from comparison as they might have microsecond differences
            compare_columns = [col for col in source_df.columns
                               if col not in ["accident_time", "road_name", "district", "date"]]

            for col in compare_columns:
                conflicts = conflicts.where(
                    f"source_df.{col} != target_df.{col}")

            if conflicts.count() > 0:
                raise Exception("Conflicts detected in the data")

    config = {
        'source_branch': source_branch,
        'target_branch': target_branch,
        'date': date
    }
    redis_client = get_redis_client()
    redis_client.rpush("silver_merge_queue_accidents_checked",
                       json.dumps(config))


if __name__ == "__main__":
    redis_client = get_redis_client()
    merge_request_str = redis_client.lpop(
        "silver_merge_queue_accidents_pre_checked")
    merge_request = json.loads(merge_request_str)

    spark = create_spark_session(
        lakefs_user["username"], lakefs_user["password"], "SilverAccidentsStagingCheckConflicts")
    check_conflicts(
        spark, merge_request["source_branch"], merge_request["target_branch"], merge_request["date"])
    spark.stop()
