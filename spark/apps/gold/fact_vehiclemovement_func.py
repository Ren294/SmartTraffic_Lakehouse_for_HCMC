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
from common import read_silver_main, write_to_warehouse, read_warehouse


def create_fact_vehicle_movement(spark, path, operator="upsert"):

    traffic_df = read_silver_main(spark, "traffic").select(
        col("license_number").alias("dim_vehicle"),
        col("email").alias("dim_owneremail"),
        col("speed_kmph").alias("fact_Speed"),
        col("street").alias("dim_loc"),
        col("timestamp").alias("dim_timestamp"),
        col("rpm").alias("fact_RPM"),
        col("oil_pressure").alias("fact_OilPressure"),
        col("fuel_level_percentage").alias("fact_FuelLevel"),
        col("passenger_count").alias("fact_PassengerCount"),
        col("internal_temperature_celsius").alias("fact_InternalTemperature"),
        col("destination_street").alias("dim_locdes"),
        col("eta").alias("fact_DestinationETA")
    )
    owner_dim = read_warehouse(spark, "dim_owner").select(
        col("OwnerKey").alias("fact_OwnerKey"),
        col("email")
    )
    loc_dim = read_warehouse(spark, "dim_location").select(
        col("LocationKey").alias("fact_LocationKey"),
        col("Street").alias("SourceStreet")
    )
    loc_dim_des = read_warehouse(spark, "dim_location").select(
        col("LocationKey").alias("fact_LocationDestinationKey"),
        col("Street").alias("DestinationStreet")
    )
    vehicle_dim = read_warehouse(spark, "dim_vehicle").select(
        col("VehicleKey").alias("fact_VehicleKey"),
        col("LicensePlate")
    )
    fact_df = traffic_df.join(
        owner_dim,
        traffic_df.dim_owneremail == owner_dim.email,
        "left"
    ).join(
        loc_dim,
        traffic_df.dim_loc == loc_dim.SourceStreet,
        "left"
    ).join(
        loc_dim_des,
        traffic_df.dim_locdes == loc_dim_des.DestinationStreet,
        "left"
    ).join(
        vehicle_dim,
        traffic_df.dim_vehicle == vehicle_dim.LicensePlate,
        "left"
    )

    fact_vehicle_movement_df = fact_df.select(
        monotonically_increasing_id().alias("MovementKey"),
        col("fact_OwnerKey").alias("OwnerKey"),
        col("fact_LocationKey").alias("LocationKey"),
        col("fact_LocationDestinationKey").alias("LocationDestinationKey"),
        col("fact_VehicleKey").alias("VehicleKey"),
        unix_timestamp(col("dim_timestamp")).alias("TimeKey"),
        col("fact_Speed").alias("Speed"),
        col("fact_RPM").alias("RPM"),
        col("fact_OilPressure").alias("OilPressure"),
        col("fact_FuelLevel").alias("FuelLevel"),
        col("fact_PassengerCount").alias("PassengerCount"),
        col("fact_InternalTemperature").alias("InternalTemperature"),
        col("fact_DestinationETA").alias("DestinationETA")
    )

    write_to_warehouse(fact_vehicle_movement_df, "fact_vehiclemovement",
                       path, recordkey="MovementKey", precombine="TimeKey", operation=operator)
