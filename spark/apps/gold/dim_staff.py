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
from pyspark.sql.types import StringType


def create_dim_staff(spark, path):
    employee_df = read_silver_main(spark, "gasstation/employee")\
        .select(
            col("employeeid").alias("StaffID"),
            col("employeename").alias("Name"),
            col("position").alias("Role"),
            col("phonenumber").alias("ContactInfo"),
            col("startdate").alias("ShiftStartTime"),
            lit(None).alias("ShiftEndTime").cast(StringType()),
            col("gasstationid").alias("Department"),
    )

    parking_feedback_df = read_silver_main(spark, "parking/feedback")\
        .select(
            col("ownerid").alias("ParkingOwnerID"),
            col("rating").alias("ParkingFeedbackRating")
    )

    staff_with_feedback_df = employee_df.join(
        parking_feedback_df,
        employee_df.StaffID == parking_feedback_df.ParkingOwnerID,
        "left"
    )

    final_dim_staff_df = staff_with_feedback_df.select(
        monotonically_increasing_id().alias("StaffKey"),
        col("StaffID"),
        col("Name"),
        col("Role"),
        col("ContactInfo"),
        col("ShiftStartTime"),
        col("ShiftEndTime"),
        col("Department"),
    )

    window_spec = Window.orderBy(col("ShiftStartTime"))
    final_dim_staff_with_segment = final_dim_staff_df.withColumn(
        "StaffSegment",
        when(row_number().over(window_spec) <= 100, "Platinum")
        .when(row_number().over(window_spec) <= 500, "Gold")
        .when(row_number().over(window_spec) <= 1000, "Silver")
        .otherwise("Bronze")
    )

    write_to_warehouse(final_dim_staff_with_segment,
                       "dim_staff", path, recordkey="StaffKey", precombine="ShiftStartTime")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_staff/"
    spark = create_spark_session(
        "DimStaff", lakefs_user["username"], lakefs_user["password"])
    create_dim_staff(spark, path)
    spark.stop()
