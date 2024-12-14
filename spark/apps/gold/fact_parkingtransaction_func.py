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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from common import read_silver_main, write_to_warehouse, read_warehouse


def create_fact_parking_transaction(spark, path, operator="upsert"):
    parkingrecord_df = read_silver_main(spark, "parking/parkingrecord")\
        .select(
            col("parkinglotid").alias("parking_parkinglotid"),
            col("vehicleid").alias("parking_vehicleid"),
            col("checkintime").alias("fact_entry_time"),
            col("checkouttime").alias("fact_exit_time"),
            col("fee").alias("fact_amount_paid"),
            lit(None).cast(StringType()).alias("fact_discount_applied"),
            lit(None).cast(StringType()).alias("fact_penalty_charges"),
            # expr("checkouttime - checkintime").alias("fact_parking_duration"),
            lit(None).cast(StringType()).alias("fact_parking_rate"),
            col("recordid").alias("parking_recordid"),
    )
    parkingvehicle_df = read_silver_main(spark, "parking/vehicle")\
        .select(
            col("vehicleid").alias("vehicleid"),
            col("licenseplate").alias("parking_licenseplate"),
    )
    parkinglot_df = read_silver_main(spark, "parking/parkinglot")\
        .select(
        split(col("location"), ", ").getItem(0).alias("location"),
        col("parkinglotid").alias("parkinglot_parkinglotid")
    )
    payment_df = read_silver_main(spark, "parking/payment")\
        .select(
            col("recordid").alias("parking_recordid"),
            col("amountpaid").alias("fact_amountpaid"),
            col("paymentmethod").alias("fact_paymentmethod"),
            col("paymentdate").alias("fact_paymentdate")
    )

    dim_vehicle_df = read_warehouse(spark, "dim_vehicle").select(
        col("VehicleKey").alias("fact_parking_vehicleid"), col("licenseplate"))

    dim_location_df = read_warehouse(spark, "dim_location").select(
        col("LocationKey").alias("fact_LocationKey"), col("Street"))

    fact_parking_transaction_df = parkingrecord_df.join(
        parkinglot_df,
        parkingrecord_df.parking_parkinglotid == parkinglot_df.parkinglot_parkinglotid,
        "left").join(
        payment_df,
        parkingrecord_df.parking_recordid == payment_df.parking_recordid,
        "left").join(
        parkingvehicle_df,
        parkingrecord_df.parking_vehicleid == parkingvehicle_df.vehicleid,
        "left").join(
        dim_vehicle_df,
        parkingvehicle_df.parking_licenseplate == dim_vehicle_df.licenseplate,
        "left")\
        .join(
        dim_location_df,
        parkinglot_df.location == dim_location_df.Street,
        "left")

    final_fact_parking_transaction = fact_parking_transaction_df.select(
        monotonically_increasing_id().alias("ParkingTransactionKey"),
        col("fact_parking_vehicleid").alias("VehicleKey"),
        col("vehicleid").alias("OwnerKey"),
        col("parking_parkinglotid").alias("ParkingLotKey"),
        unix_timestamp(col("fact_entry_time")).alias("TimeKey"),
        col("fact_LocationKey").alias("LocationKey"),
        col("fact_amount_paid").alias("amount_paid"),
        expr("CAST(UNIX_TIMESTAMP(fact_exit_time) - UNIX_TIMESTAMP(fact_entry_time) AS LONG)").alias(
            "parking_duration"),
        col("fact_parking_rate").alias("parking_rate"),
        col("fact_entry_time").alias("entry_time"),
        col("fact_exit_time").alias("exit_time"),
        col("fact_paymentmethod").alias("payment_method"),
        col("fact_discount_applied").alias("discount_applied"),
        col("fact_penalty_charges").alias("penalty_charges")
    )

    write_to_warehouse(final_fact_parking_transaction, "fact_parkingtransaction",
                       path, recordkey="ParkingTransactionKey", precombine="entry_time", operation=operator)
