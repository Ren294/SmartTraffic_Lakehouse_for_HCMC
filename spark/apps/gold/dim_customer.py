from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse, create_table_warehouse


def create_dim_customer(spark, path):
    gas_customer_df = read_silver_main(spark, "gasstation/customer/")\
        .select(
            col("customerid").alias("gas_customerid"),
            col("customername").alias("gas_name"),
            col("email").alias("gas_email"),
            col("phonenumber").alias("gas_phone"),
            col("address").alias("gas_address"),
            col("vehicletypename").alias("gas_vehicle_type"),
            col("licenseplate").alias("gas_license_plate"),
            col("last_update").alias("gas_registration_date")
    )

    parking_owner_df = read_silver_main(spark, "parking/owner/")\
        .select(
            col("ownerid").alias("parking_ownerid"),
            col("name").alias("parking_name"),
            col("email").alias("parking_email"),
            col("phonenumber").alias("parking_phone"),
            col("contactinfo").alias("parking_contact_info")
    )

    parking_vehicle_df = read_silver_main(spark, "parking/vehicle/")\
        .select(
            col("ownerid").alias("vehicle_ownerid"),
            col("vehicletype").alias("parking_vehicle_type"),
            col("licenseplate").alias("parking_license_plate"),
            col("color").alias("vehicle_color"),
            col("brand").alias("vehicle_brand")
    )

    parking_combined_df = parking_owner_df.join(
        parking_vehicle_df,
        parking_owner_df.parking_ownerid == parking_vehicle_df.vehicle_ownerid,
        "left"
    )

    dim_customer_df = gas_customer_df.fullOuterJoin(
        parking_combined_df,
        gas_customer_df.gas_email == parking_combined_df.parking_email,
        "full"
    )

    final_dim_customer = dim_customer_df.select(
        monotonically_increasing_id().alias("CustomerKey"),

        coalesce(col("gas_customerid"), col(
            "parking_ownerid")).alias("CustomerID"),

        coalesce(col("gas_name"), col("parking_name")).alias("Name"),

        coalesce(col("gas_phone"), col("parking_phone")).alias("PhoneNumber"),
        coalesce(col("gas_email"), col("parking_email")).alias("Email"),

        coalesce(col("gas_address"), col(
            "parking_contact_info")).alias("Address"),

        coalesce(col("gas_vehicle_type"), col(
            "parking_vehicle_type")).alias("VehicleTypeName"),

        coalesce(col("gas_license_plate"), col(
            "parking_license_plate")).alias("LicensePlate"),

        coalesce(col("gas_registration_date"),
                 current_timestamp()).alias("RegistrationDate")
    )

    segment_window = Window.orderBy(col("RegistrationDate"))
    final_dim_customer_with_segment = final_dim_customer.withColumn(
        "CustomerSegment",
        when(row_number().over(segment_window) <= 100, "Platinum")
        .when(row_number().over(segment_window) <= 500, "Gold")
        .when(row_number().over(segment_window) <= 1000, "Silver")
        .otherwise("Bronze")
    )

    write_to_warehouse(final_dim_customer_with_segment,
                       "Dim_Customer", path, recordkey="CustomerKey", precombine="RegistrationDate")

    create_table_warehouse(spark, "dim_customer", path)


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_customer/"
    spark = create_spark_session(
        "DimCustomers", lakefs_user["username"], lakefs_user["password"])
    create_dim_customer(spark, path)
    spark.stop()
