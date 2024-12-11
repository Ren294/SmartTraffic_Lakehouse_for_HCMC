"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from common import create_spark_session, get_lakefs
from fact_parkingtransaction_func import create_fact_parking_transaction
if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/fact_parkingtransaction/"
    spark = create_spark_session(
        "FactParkingTransaction", lakefs_user["username"], lakefs_user["password"])
    create_fact_parking_transaction(spark, path, "bulk_insert")
    spark.stop()
