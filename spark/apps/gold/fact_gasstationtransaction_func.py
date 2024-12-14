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
from common import read_silver_main, write_to_warehouse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


def create_fact_gas_station_sales(spark, path, operator="upsert"):

    invoice_df = read_silver_main(spark, "gasstation/invoice").select(
        col("invoiceid").alias("InvoiceID"),
        col("customerid").alias("CustomerID"),
        col("employeeid").alias("EmployeeID"),
        col("gasstationid").alias("GasStationID"),
        col("issuedate").alias("TimeKey"),
        col("totalamount").alias("TotalAmount"),
        col("paymentmethod").alias("PaymentMethod")
    )

    invoice_detail_df = read_silver_main(spark, "gasstation/invoicedetail").select(
        col("invoicedetailid").alias("TransactionKey"),
        col("invoiceid").alias("InvoiceID"),
        col("productid").alias("ProductKey"),
        col("quantitysold").alias("QuantitySold"),
        col("sellingprice").alias("UnitPrice"),
        col("totalprice").alias("DetailTotalAmount")
    )

    sales_fact_df = invoice_df.join(
        invoice_detail_df, "InvoiceID", "inner"
    ).withColumn(
        "TaxAmount", col("DetailTotalAmount") * lit(0.1)
    ).withColumn(
        "Discount", lit(0).cast(IntegerType())
    ).withColumn(
        "PromotionID", lit(None).cast(StringType())
    )

    final_fact_gas_station_sales = sales_fact_df.select(
        col("TransactionKey"),
        col("CustomerID").alias("CustomerKey"),
        col("EmployeeID").alias("EmployeeKey"),
        col("GasStationID").alias("GasStationKey"),
        col("ProductKey"),
        col("TimeKey"),
        col("QuantitySold"),
        col("UnitPrice"),
        col("DetailTotalAmount").alias("TotalAmount"),
        col("Discount"),
        col("TaxAmount"),
        col("PaymentMethod"),
        col("PromotionID")
    )

    write_to_warehouse(final_fact_gas_station_sales,
                       "fact_gasstationtransaction", path, recordkey="TransactionKey", precombine="TimeKey", operation=operator)
