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
    gasstation_df = read_silver_main(spark, "gasstation/gasstation") \
        .select(
            split(col("address"), ", ").getItem(0).alias("Street"),
            split(col("address"), ", ").getItem(1).alias("District"),
            split(col("address"), ", ").getItem(2).alias("City"),
            lit(None).alias("Latitude"),
            lit(None).alias("Longitude"),
            lit('00700').alias("PostalCode"),
            lit("Vietnam").alias("Country"),
            lit("Ho Chi Minh").alias("Region"),
            lit(None).alias("Geospatial_Coordinates")
    )

    parking_location_df = read_silver_main(spark, "parking/parkinglot") \
        .select(
            split(col("location"), ", ").getItem(0).alias("Street"),
            split(col("location"), ", ").getItem(1).alias("District"),
            split(col("location"), ", ").getItem(2).alias("City"),
            lit(None).alias("Latitude"),
            lit(None).alias("Longitude"),
            lit('00700').alias("PostalCode"),
            lit("Vietnam").alias("Country"),
            lit("Ho Chi Minh").alias("Region"),
            lit(None).alias("Geospatial_Coordinates")
    )

    accident_location_df = read_silver_main(spark, "accidents") \
        .select(
            col("road_name").alias("Street"),
            col("district").alias("District"),
            col("city").alias("City"),
            lit(None).alias("Latitude"),
            lit(None).alias("Longitude"),
            lit(None).alias("PostalCode"),
            lit("Vietnam").alias("Country"),
            lit("Ho Chi Minh").alias("Region"),
            lit(None).alias("Geospatial_Coordinates")
    )

    weather_location_df = read_silver_main(spark, "weather") \
        .select(
            col("address").alias("Street"),
            lit(None).alias("District"),
            lit("Ho Chi Minh").alias("City"),
            lit(None).alias("Latitude"),
            lit(None).alias("Longitude"),
            lit('00700').alias("PostalCode"),
            lit("Vietnam").alias("Country"),
            lit("Ho Chi Minh").alias("Region"),
            concat_ws(", ", col("latitude"), col("longitude")
                      ).alias("Geospatial_Coordinates")
    )

    traffic_location_df = read_silver_main(spark, "traffic") \
        .select(
            col("street").alias("Street"),
            col("district").alias("District"),
            col("city").alias("City"),
            col("latitude").alias("Latitude"),
            col("longitude").alias("Longitude"),
            lit('00700').alias("PostalCode"),
            lit("Vietnam").alias("Country"),
            lit("Ho Chi Minh").alias("Region"),
            concat_ws(", ", col("latitude"), col("longitude")
                      ).alias("Geospatial_Coordinates")
    )

    combined_df = gasstation_df.unionByName(parking_location_df, allowMissingColumns=True) \
        .unionByName(accident_location_df, allowMissingColumns=True) \
        .unionByName(weather_location_df, allowMissingColumns=True) \
        .unionByName(traffic_location_df, allowMissingColumns=True)

    final_dim_location = combined_df.withColumn(
        "LocationKey", monotonically_increasing_id())

    write_to_warehouse(final_dim_location, "dim_location",
                       path, recordkey="LocationKey", precombine="LocationKey")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_location/"
    spark = create_spark_session(
        "DimLocation", lakefs_user["username"], lakefs_user["password"])
    create_dim_location(spark, path)
    spark.stop()
