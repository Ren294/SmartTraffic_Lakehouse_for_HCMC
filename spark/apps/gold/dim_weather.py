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

    weather_df = read_silver_main(spark, "weather")\
        .select(
            col("date").alias("weather_date"),
            col("datetime").alias("weather_datetime"),
            col("temp").alias("temperature"),
            col("feelslike").alias("feels_like_temperature"),
            col("humidity").alias("humidity"),
            col("precipprob").alias("precipitation"),
            col("preciptype").alias("precipitation_type"),
            col("snowdepth").alias("snow_depth"),
            col("windspeed").alias("wind_speed"),
            col("winddir").alias("wind_direction"),
            col("pressure").alias("pressure"),
            col("visibility").alias("visibility"),
            col("cloudcover").alias("cloud_cover"),
            col("solarradiation").alias("solar_radiation"),
            col("uvindex").alias("uv_index"),
            col("conditions").alias("conditions"),
            col("severerisk").alias("severe_risk")
    )\
        .withColumn(
            "DateTime",
            to_timestamp(
                concat_ws(
                    " ",
                    col("weather_date"),
                    date_format(col("weather_datetime"), "HH:mm:ss")
                ), "yyyy-MM-dd HH:mm:ss"
            )
    )

    dim_weather_df = weather_df.select(
        monotonically_increasing_id().alias("WeatherKey"),
        col("DateTime"),
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

    write_to_warehouse(dim_weather_df, "dim_weather", path,
                       recordkey="WeatherKey", precombine="DateTime")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_weather/"
    spark = create_spark_session(
        "DimWeather", lakefs_user["username"], lakefs_user["password"])
    create_dim_weather(spark, path)
    spark.stop()
