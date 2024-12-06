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


def create_dim_location(spark, path):
    gasstation_df = read_silver_main(spark, "gasstation/gasstation").select(
        col("gasstationid").alias("station_id"),
        col("address").alias("station_address")
    )

    parking_df = read_silver_main(spark, "parking/parkinglot").select(
        col("parkinglotid").alias("parking_id"),
        col("address").alias("parking_address")
    )

    accident_df = read_silver_main(spark, "accident").select(
        col("accidentid").alias("accident_id"),
        col("street").alias("accident_street"),
        col("district").alias("accident_district"),
        col("city").alias("accident_city"),
        col("latitude").alias("accident_latitude"),
        col("longitude").alias("accident_longitude"),
        col("postalcode").alias("accident_postalcode")
    )

    weather_df = read_silver_main(spark, "weather").select(
        col("stationid").alias("weather_station_id"),
        col("street").alias("weather_street"),
        col("district").alias("weather_district"),
        col("city").alias("weather_city"),
        col("latitude").alias("weather_latitude"),
        col("longitude").alias("weather_longitude")
    )

    traffic_df = read_silver_main(spark, "traffic").select(
        col("trafficid").alias("traffic_id"),
        col("street").alias("traffic_street"),
        col("district").alias("traffic_district"),
        col("city").alias("traffic_city"),
        col("latitude").alias("traffic_latitude"),
        col("longitude").alias("traffic_longitude")
    )

    combined_df = gasstation_df \
        .unionByName(parking_df, allowMissingColumns=True) \
        .unionByName(accident_df, allowMissingColumns=True) \
        .unionByName(weather_df, allowMissingColumns=True) \
        .unionByName(traffic_df, allowMissingColumns=True)

    dim_location_df = combined_df.select(
        monotonically_increasing_id().alias("LocationKey"),
        col("station_address").alias("Street"),
        col("accident_district").alias("District"),
        col("accident_city").alias("City"),
        col("accident_latitude").alias("Latitude"),
        col("accident_longitude").alias("Longitude"),
        col("accident_postalcode").alias("PostalCode"),
        lit("Vietnam").alias("Country"),
        lit("Asia").alias("Region"),
        concat_ws(", ", col("Latitude"), col("Longitude")
                  ).alias("Geospatial_Coordinates")
    )

    write_to_warehouse(dim_location_df, "dim_location", path,
                       recordkey="LocationKey", precombine="LocationKey")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_location/"
    spark = create_spark_session(
        "DimLocation", lakefs_user["username"], lakefs_user["password"])
    create_dim_location(spark, path)
    spark.stop()
