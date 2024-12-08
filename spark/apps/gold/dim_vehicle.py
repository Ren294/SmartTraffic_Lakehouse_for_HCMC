"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse


def create_dim_vehicle(spark, path):
    gas_vehicle_df = read_silver_main(spark, "gasstation/customer").select(
        col("licenseplate").alias("LicensePlate"),
        col("vehicletypename").alias("VehicleType"),
        lit("vehicletypename").alias("VehicleClassification"),
        lit(None).alias("Brand"),
        lit(None).alias("Model"),
        lit(None).alias("Color"),
        lit(2010).alias("ManufactureYear"),
        lit('GASOLINE').alias("FuelType"),
        col("last_update").alias("RegistrationDate"),
        lit(300).alias("VehicleLength"),
        lit(200).alias("VehicleWidth"),
        lit(350).alias("VehicleHeight")
    )

    parking_vehicle_df = read_silver_main(spark, "parking/vehicle").select(
        col("licenseplate").alias("LicensePlate"),
        col("vehicletype").alias("VehicleType"),
        lit("vehicletype").alias("VehicleClassification"),
        col("brand").alias("Brand"),
        col("brand").alias("Model"),
        col("color").alias("Color"),
        lit(2010).alias("ManufactureYear"),
        lit('GASOLINE').alias("FuelType"),
        current_timestamp().alias("RegistrationDate"),
        lit(300).alias("VehicleLength"),
        lit(200).alias("VehicleWidth"),
        lit(350).alias("VehicleHeight")
    )

    traffic_vehicle_df = read_silver_main(spark, "traffic").select(
        col("license_number").alias("LicensePlate"),
        col("vehicle_type").alias("VehicleType"),
        lit("vehicle_classification").alias("VehicleClassification"),
        lit(None).alias("Brand"),
        lit(None).alias("Model"),
        lit(None).alias("Color"),
        lit(2010).alias("ManufactureYear"),
        lit('GASOLINE').alias("FuelType"),
        current_timestamp().alias("RegistrationDate"),
        (col("length_meters").cast(IntegerType())*100).alias("VehicleLength"),
        (col("width_meters").cast(IntegerType())*100).alias("VehicleWidth"),
        (col("height_meters").cast(IntegerType())*100).alias("VehicleHeight")
    )
    combined_vehicle_df = gas_vehicle_df\
        .unionByName(
            parking_vehicle_df, allowMissingColumns=True)\
        .unionByName(
            traffic_vehicle_df, allowMissingColumns=True)

    final_dim_vehicle = combined_vehicle_df.withColumn(
        "VehicleKey", monotonically_increasing_id()
    ).dropDuplicates(["LicensePlate"])

    write_to_warehouse(
        final_dim_vehicle,
        "dim_vehicle",
        path,
        recordkey="VehicleKey",
        precombine="RegistrationDate"
    )


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_vehicle/"
    spark = create_spark_session(
        "DimVehicle", lakefs_user["username"], lakefs_user["password"])
    create_dim_vehicle(spark, path)
    spark.stop()
