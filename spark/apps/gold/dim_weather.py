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


def create_dim_weather(spark, path):
    weather_df = read_silver_main(spark, "weather/observations").select(
        col("datetime").alias("DateTime"),
        col("temperature").alias("Temperature"),
        col("feels_like_temperature").alias("FeelsLikeTemperature"),
        col("humidity").alias("Humidity"),
        col("precipitation").alias("Precipitation"),
        col("precipitation_type").alias("PrecipitationType"),
        col("snow_depth").alias("SnowDepth"),
        col("wind_speed").alias("WindSpeed"),
        col("wind_direction").alias("WindDirection"),
        col("pressure").alias("Pressure"),
        col("visibility").alias("Visibility"),
        col("cloud_cover").alias("CloudCover"),
        col("solar_radiation").alias("SolarRadiation"),
        col("uv_index").alias("UVIndex"),
        col("conditions").alias("Conditions"),
        col("severe_risk").alias("SevereRisk")
    )

    traffic_df = read_silver_main(spark, "traffic/congestion").select(
        col("datetime").alias("TrafficDateTime"),
        col("congestion_level").alias("CongestionLevel")
    )

    accident_df = read_silver_main(spark, "accident/data").select(
        col("datetime").alias("AccidentDateTime"),
        col("severity").alias("AccidentSeverity")
    )

    combined_df = weather_df.join(
        traffic_df, weather_df.DateTime == traffic_df.TrafficDateTime, "left"
    ).join(
        accident_df, weather_df.DateTime == accident_df.AccidentDateTime, "left"
    )

    final_dim_weather = combined_df.select(
        monotonically_increasing_id().alias("WeatherKey"),
        col("DateTime"),
        col("Temperature"),
        col("FeelsLikeTemperature"),
        col("Humidity"),
        col("Precipitation"),
        col("PrecipitationType"),
        col("SnowDepth"),
        col("WindSpeed"),
        col("WindDirection"),
        col("Pressure"),
        col("Visibility"),
        col("CloudCover"),
        col("SolarRadiation"),
        col("UVIndex"),
        col("Conditions"),
        col("SevereRisk")
    )

    write_to_warehouse(final_dim_weather, "dim_weather", path,
                       recordkey="WeatherKey", precombine="DateTime")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_weather/"
    spark = create_spark_session(
        "DimWeather", lakefs_user["username"], lakefs_user["password"])
    create_dim_weather(spark, path)
    spark.stop()
