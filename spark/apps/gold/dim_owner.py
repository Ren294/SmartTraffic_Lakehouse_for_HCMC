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


def create_dim_owner(spark, path):
    gas_customer_df = read_silver_main(spark, "gasstation/customer")\
        .select(
            col("customerid").alias("ownerid"),
            col("customername").alias("name"),
            col("email").alias("email"),
            col("phonenumber").alias("phone"),
            col("address").alias("contactinfo"),
            lit("GasStation").alias("ownertype"),
            col("last_update").alias("registration_date")
    )

    parking_owner_df = read_silver_main(spark, "parking/owner")\
        .select(
            col("ownerid").alias("ownerid"),
            col("name").alias("name"),
            col("email").alias("email"),
            col("phonenumber").alias("phone"),
            col("contactinfo").alias("contactinfo"),
            lit("Parking").alias("ownertype"),
            col("last_update").alias("registration_date")
    )

    parking_vehicle_df = read_silver_main(spark, "parking/vehicle")\
        .select(
            col("ownerid").alias("vehicle_ownerid"),
            col("licenseplate").alias("vehicle_license_plate")
    )

    combined_owner_df = gas_customer_df.union(parking_owner_df)

    owner_with_vehicle_df = combined_owner_df.join(
        parking_vehicle_df,
        combined_owner_df.ownerid == parking_vehicle_df.vehicle_ownerid,
        "left"
    )

    final_dim_owner_df = owner_with_vehicle_df.select(
        monotonically_increasing_id().alias("OwnerKey"),
        col("ownerid"),
        col("name"),
        col("contactinfo"),
        col("email"),
        col("phone"),
        col("ownertype"),
        col("registration_date")
    )

    segment_window = Window.orderBy(col("registration_date"))
    final_dim_owner_with_segment = final_dim_owner_df.withColumn(
        "OwnerSegment",
        when(row_number().over(segment_window) <= 100, "Platinum")
        .when(row_number().over(segment_window) <= 500, "Gold")
        .when(row_number().over(segment_window) <= 1000, "Silver")
        .otherwise("Bronze")
    )

    write_to_warehouse(final_dim_owner_with_segment,
                       "dim_owner", path, recordkey="OwnerKey", precombine="registration_date")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_owner/"
    spark = create_spark_session(
        "DimOwner", lakefs_user["username"], lakefs_user["password"])
    create_dim_owner(spark, path)
    spark.stop()
