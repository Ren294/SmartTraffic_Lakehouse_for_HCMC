"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse


def create_dim_parkinglot(spark, path):
    parking_lot_df = read_silver_main(spark, "parking/parkinglot") \
        .select(
            col("parkinglotid").alias("ParkingLotID"),
            col("name").alias("Name"),
            col("location").alias("Location"),
            col("totalspaces").alias("TotalSpaces"),
            col("carspaces").alias("CarSpaces"),
            col("motorbikespaces").alias("MotorbikeSpaces"),
            col("bicyclespaces").alias("BicycleSpaces"),
            col("type").alias("Type"),
            col("hourlyrate").alias("HourlyRate"),
            col("last_update").alias("LastUpdate")
    )

    final_dim_parkinglot = parking_lot_df.select(
        monotonically_increasing_id().alias("ParkingLotKey"),
        col("ParkingLotID"),
        col("Name"),
        col("Location"),
        col("TotalSpaces"),
        col("CarSpaces"),
        col("MotorbikeSpaces"),
        col("BicycleSpaces"),
        col("Type"),
        col("HourlyRate"),
        col("LastUpdate"),

    )

    write_to_warehouse(final_dim_parkinglot, "dim_parkinglot",
                       path, recordkey="ParkingLotKey", precombine="LastUpdate")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_parking_lot/"
    spark = create_spark_session(
        "DimParkingLot", lakefs_user["username"], lakefs_user["password"])
    create_dim_parkinglot(spark, path)
    spark.stop()
