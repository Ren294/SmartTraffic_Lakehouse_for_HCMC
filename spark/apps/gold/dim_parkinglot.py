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


def create_dim_parking_lot(spark, path):
    gasstation_df = read_silver_main(spark, "gasstation/gasstation")\
        .select(
            col("gasstationid").alias("ParkingLotID"),
            col("gasstationname").alias("Name"),
            col("address").alias("Location"),
            col("phonenumber").alias("PhoneNumber"),
            col("email").alias("Email"),
            col("last_update").alias("LastUpdated")
    )

    parking_lot_df = read_silver_main(spark, "parking/lot")\
        .select(
            col("parkinglotid").alias("ParkingLotID"),
            col("totalslots").alias("TotalSpaces"),
            col("carslots").alias("CarSpaces"),
            col("motorbikeslots").alias("MotorbikeSpaces"),
            col("bicycleslots").alias("BicycleSpaces"),
            col("type").alias("Type"),
            col("hourlyrate").alias("HourlyRate"),
            col("operatinghours").alias("OperatingHours"),
            col("securityfeatures").alias("SecurityFeatures")
    )

    accident_df = read_silver_main(spark, "accident")\
        .select(
            col("accidentid").alias("AccidentID"),
            col("parkinglotid").alias("ParkingLotID"),
            col("accidentdate").alias("AccidentDate")
    )

    dim_parking_lot_df = gasstation_df.join(
        parking_lot_df,
        "ParkingLotID",
        "left"
    )

    dim_parking_lot_with_accidents = dim_parking_lot_df.join(
        accident_df,
        "ParkingLotID",
        "left"
    )

    final_dim_parking_lot_df = dim_parking_lot_with_accidents.select(
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
        col("OperatingHours"),
        col("SecurityFeatures")
    )

    write_to_warehouse(final_dim_parking_lot_df,
                       "dim_parking_lot", path, recordkey="ParkingLotKey", precombine="LastUpdated")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_parking_lot/"
    spark = create_spark_session(
        "DimParkingLot", lakefs_user["username"], lakefs_user["password"])
    create_dim_parking_lot(spark, path)
    spark.stop()
