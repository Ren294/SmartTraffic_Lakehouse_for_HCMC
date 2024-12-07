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
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse, create_table_warehouse


def create_dim_employee(spark, path):
    gas_employee_df = read_silver_main(spark, "gasstation/employee")\
        .select(
            col("employeeid").alias("gas_employeeid"),
            col("employeename").alias("gas_name"),
            col("position").alias("gas_position"),
            col("phonenumber").alias("gas_phone"),
            col("email").alias("gas_email"),
            col("startdate").alias("gas_start_date"),
            lit(None).cast(StringType()).alias("gas_end_time"),
            col("address").alias("gas_address"),
    )

    parking_employee_df = read_silver_main(spark, "parking/staff")\
        .select(
            col("staffid").alias("parking_employeeid"),
            col("name").alias("parking_name"),
            col("role").alias("parking_position"),
            col("contactinfo").alias("parking_phone"),
            col("contactinfo").alias("parking_email"),
            col("shiftstarttime").alias("parking_start_date"),
            col("shiftendtime").alias("parking_end_time"),
            col("contactinfo").alias("parking_address")
    )

    dim_employee_df = gas_employee_df.join(
        parking_employee_df,
        gas_employee_df.gas_email == parking_employee_df.parking_email,
        "full_outer"
    )

    final_dim_employee = dim_employee_df.select(
        monotonically_increasing_id().alias("EmployeeKey"),
        coalesce(col("gas_employeeid"), col(
            "parking_employeeid")).alias("EmployeeID"),
        coalesce(col("gas_name"), col("parking_name")).alias("Name"),
        coalesce(col("gas_position"), col(
            "parking_position")).alias("Position"),
        coalesce(col("gas_phone"), col("parking_phone")).alias("PhoneNumber"),
        coalesce(col("gas_email"), col("parking_email")).alias("Email"),
        coalesce(col("gas_start_date"), col(
            "parking_start_date"), current_timestamp()).alias("StartDate"),
        coalesce(col("gas_address"), col(
            "parking_address")).alias("Address")
    )

    write_to_warehouse(final_dim_employee,
                       "dim_employee", path, recordkey="EmployeeKey", precombine="StartDate")

    # create_table_warehouse(spark, "dim_employee", path)


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_employee/"
    spark = create_spark_session(
        "DimEmployees", lakefs_user["username"], lakefs_user["password"])
    create_dim_employee(spark, path)
    spark.stop()
