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
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse


def create_dim_gas_station(spark, path):
    gas_station_df = read_silver_main(spark, "gasstation/gasstation").select(
        col("gasstationid").alias("GasStationID"),
        col("gasstationname").alias("GasStationName"),
        col("address").alias("Address"),
        col("phonenumber").alias("PhoneNumber"),
        col("email").alias("Email"),
        col("last_update").alias("LastUpdate")
    )

    total_tanks_df = read_silver_main(spark, "gasstation/inventorytransaction")\
        .groupBy("tankid")\
        .agg(countDistinct("tankid").alias("TotalTanks"))

    gas_station_geo_df = gas_station_df.withColumn(
        "GeographicRegion",
        when(col("Address").contains("Ho Chi Minh"), "Ho Chi Minh")
        .when(col("Address").contains("Hanoi"), "Hanoi")
        .otherwise("Other")
    )

    facilities_df = read_silver_main(spark, "gasstation/employee")\
        .groupBy("gasstationid")\
        .agg(collect_list("position").alias("Facilities"))\
        .withColumn("Facilities", concat_ws(", ", col("Facilities")))

    dim_gas_station_df = gas_station_geo_df.join(
        total_tanks_df,
        gas_station_geo_df.GasStationID == total_tanks_df.tankid,
        "left"
    ).join(
        facilities_df,
        gas_station_geo_df.GasStationID == facilities_df.gasstationid,
        "left"
    ).select(
        monotonically_increasing_id().alias("GasStationKey"),
        gas_station_geo_df["GasStationID"],
        col("GasStationName"),
        col("Address"),
        col("PhoneNumber"),
        col("Email"),
        coalesce(col("TotalTanks"), lit(0)).alias("TotalTanks"),
        col("LastUpdate").alias("OperatingHours"),
        col("Facilities"),
        col("GeographicRegion")
    )

    write_to_warehouse(dim_gas_station_df,
                       "dim_gas_station", path, recordkey="GasStationKey", precombine="OperatingHours")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_gas_station/"
    spark = create_spark_session(
        "DimGasStation", lakefs_user["username"], lakefs_user["password"])
    create_dim_gas_station(spark, path)
    spark.stop()
