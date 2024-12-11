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
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse, create_table_warehouse
from fact_gasstationtransaction_func import create_fact_gas_station_sales

if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/fact_gasstationtransaction/"
    spark = create_spark_session(
        "FactGasStationTransaction", lakefs_user["username"], lakefs_user["password"])
    create_fact_gas_station_sales(spark, path)
    spark.stop()
