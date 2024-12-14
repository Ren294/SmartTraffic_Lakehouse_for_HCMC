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


def create_fact_traffic_incident(spark, path, operator="upsert"):
    accident_df = read_silver_main(spark, "accidents").select(
        col("road_name").alias("Location"),
        col("accident_time").alias("fact_AccidentDateTime"),
        col("number_of_vehicles").alias("NumberOfVehicles"),
        col("car_involved").alias("CarInvolved"),
        col("motobike_involved").alias("MotorbikeInvolved"),
        col("other_involved").alias("OtherVehiclesInvolved"),
        col("accident_severity").alias("AccidentSeverity"),
        col("congestion_km").alias("CongestionKM"),
        col("estimated_recovery_time").alias("EstimatedRecoveryTime"),
        col("description").alias("IncidentDescription"),
        lit('Normal').alias("RoadCondition"),
        lit(None).cast(StringType()).alias("TrafficControl")
    )
    location_dim = read_warehouse(spark, "dim_location").select(
        col("Street"),
        col("LocationKey").alias("fact_LocationKey")
    )

    weather_dim = read_warehouse(spark, "dim_weather").select(
        col("WeatherKey").alias("fact_WeatherKey"),
        col("DateTime").alias("WeatherDateTime"),
    )

    fact_df = accident_df.join(
        location_dim,
        accident_df.Location == location_dim.Street,
        "left"
    ).join(
        weather_dim,
        accident_df.fact_AccidentDateTime == weather_dim.WeatherDateTime,
        "left"
    )

    final_fact_traffic_incident = fact_df.select(
        monotonically_increasing_id().alias("IncidentKey"),
        col("fact_LocationKey").alias("LocationKey"),
        unix_timestamp(col("fact_AccidentDateTime")).alias("TimeKey"),
        col("fact_WeatherKey").alias("WeatherKey"),
        col("NumberOfVehicles"),
        col("CarInvolved"),
        col("MotorbikeInvolved"),
        col("OtherVehiclesInvolved"),
        col("AccidentSeverity"),
        col("CongestionKM"),
        col("EstimatedRecoveryTime"),
        col("IncidentDescription"),
        col("RoadCondition"),
        col("TrafficControl")
    )
    write_to_warehouse(final_fact_traffic_incident, "fact_trafficincident", path,
                       recordkey="IncidentKey", precombine="TimeKey", operation=operator)
