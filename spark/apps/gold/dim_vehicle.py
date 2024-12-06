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


def create_dim_vehicle(spark, path):
    gas_customer_df = read_silver_main(spark, "gasstation/customer")\
        .select(
            col("customerid").alias("gas_customerid"),
            col("licenseplate").alias("gas_license_plate"),
            col("vehicletypename").alias("gas_vehicle_type"),
            col("last_update").alias("gas_registration_date")
    )

    parking_vehicle_df = read_silver_main(spark, "parking/vehicle")\
        .select(
            col("vehicleid").alias("parking_vehicle_id"),
            col("licenseplate").alias("parking_license_plate"),
            col("vehicletype").alias("parking_vehicle_type"),
            col("brand").alias("parking_brand"),
            col("model").alias("parking_model"),
            col("color").alias("parking_color"),
            col("manufactureyear").alias("parking_manufacture_year"),
            col("last_update").alias("parking_registration_date")
    )

    accident_vehicle_df = read_silver_main(spark, "accident")\
        .select(
            col("vehicleid").alias("accident_vehicle_id"),
            col("licenseplate").alias("accident_license_plate"),
            col("vehicleclassification").alias(
                "accident_vehicle_classification"),
            col("fueltype").alias("accident_fuel_type"),
            col("vehiclelength").alias("accident_vehicle_length"),
            col("vehiclewidth").alias("accident_vehicle_width"),
            col("vehicleheight").alias("accident_vehicle_height"),
            col("last_update").alias("accident_registration_date")
    )

    combined_df = gas_customer_df.join(
        parking_vehicle_df,
        gas_customer_df.gas_license_plate == parking_vehicle_df.parking_license_plate,
        "full_outer"
    ).join(
        accident_vehicle_df,
        coalesce(gas_customer_df.gas_license_plate,
                 parking_vehicle_df.parking_license_plate) == accident_vehicle_df.accident_license_plate,
        "full_outer"
    )

    dim_vehicle_df = combined_df.select(
        monotonically_increasing_id().alias("VehicleKey"),
        coalesce(col("gas_customerid"), col("parking_vehicle_id"),
                 col("accident_vehicle_id")).alias("VehicleID"),
        coalesce(col("gas_license_plate"), col("parking_license_plate"),
                 col("accident_license_plate")).alias("LicensePlate"),
        coalesce(col("gas_vehicle_type"), col(
            "parking_vehicle_type")).alias("VehicleType"),
        col("accident_vehicle_classification").alias("VehicleClassification"),
        coalesce(col("parking_brand"), lit("Unknown")).alias("Brand"),
        coalesce(col("parking_model"), lit("Unknown")).alias("Model"),
        coalesce(col("parking_color"), lit("Unknown")).alias("Color"),
        col("parking_manufacture_year").alias("ManufactureYear"),
        col("accident_fuel_type").alias("FuelType"),
        coalesce(col("gas_registration_date"), col("parking_registration_date"), col(
            "accident_registration_date"), current_timestamp()).alias("RegistrationDate"),
        col("accident_vehicle_length").alias("VehicleLength"),
        col("accident_vehicle_width").alias("VehicleWidth"),
        col("accident_vehicle_height").alias("VehicleHeight")
    )

    write_to_warehouse(dim_vehicle_df, "dim_vehicle", path,
                       recordkey="VehicleKey", precombine="RegistrationDate")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_vehicle/"
    spark = create_spark_session(
        "DimVehicle", lakefs_user["username"], lakefs_user["password"])
    create_dim_vehicle(spark, path)
    spark.stop()
