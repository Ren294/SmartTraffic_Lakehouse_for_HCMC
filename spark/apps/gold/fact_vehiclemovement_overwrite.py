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
from common import create_spark_session, get_lakefs

from fact_vehiclemovement_func import create_fact_vehicle_movement
if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/fact_vehiclemovement/"
    spark = create_spark_session(
        "FactVehicleMovement", lakefs_user["username"], lakefs_user["password"])
    create_fact_vehicle_movement(spark, path, "bulk_insert")
    spark.stop()
