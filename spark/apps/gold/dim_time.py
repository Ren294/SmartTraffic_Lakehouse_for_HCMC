"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format, when, month, year, quarter, dayofmonth, \
    dayofweek, dayofyear, hour, minute, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import datetime
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse, create_table_warehouse


def create_dim_time(spark, path):
    start_date = datetime.datetime(2024, 1, 1, 0, 0, 0)
    end_date = datetime.datetime(2024, 6, 30, 23, 59, 59)
    time_data = []
    current_time = start_date
    while current_time <= end_date:
        time_data.append((current_time,))
        current_time += datetime.timedelta(minutes=1)

    schema = StructType([
        StructField("Timestamp", TimestampType(), True)
    ])

    time_df = spark.createDataFrame(time_data, schema)

    dim_time_df = time_df \
        .withColumn("TimeKey", unix_timestamp(col("Timestamp")).cast(IntegerType())) \
        .withColumn("Date", date_format(col("Timestamp"), "yyyy-MM-dd")) \
        .withColumn("Year", year(col("Timestamp"))) \
        .withColumn("Quarter", quarter(col("Timestamp"))) \
        .withColumn("Month", month(col("Timestamp"))) \
        .withColumn("MonthName", date_format(col("Timestamp"), "MMMM")) \
        .withColumn("Day", dayofmonth(col("Timestamp"))) \
        .withColumn("DayOfWeek", dayofweek(col("Timestamp"))) \
        .withColumn("DayOfYear", dayofyear(col("Timestamp"))) \
        .withColumn("Hour", hour(col("Timestamp"))) \
        .withColumn("Minute", minute(col("Timestamp"))) \
        .withColumn("IsWeekend", when(col("DayOfWeek").isin(1, 7), lit(1)).otherwise(lit(0))) \
        .withColumn("IsHoliday", lit(0))  \
        .withColumn("SeasonName", when((col("Month").isin(1, 2, 3)), lit("Spring"))
                    .when((col("Month").isin(4, 5, 6)), lit("Summer"))
                    .when((col("Month").isin(7, 8, 9)), lit("Autumn"))
                    .otherwise(lit("Autumn")))

    write_to_warehouse(dim_time_df,
                       "dim_time", path, recordkey="TimeKey", precombine="TimeKey")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_time/"
    spark = create_spark_session(
        "DimTime", lakefs_user["username"], lakefs_user["password"])
    create_dim_time(spark, path)
    spark.stop()
