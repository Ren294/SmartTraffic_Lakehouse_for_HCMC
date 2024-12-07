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


def create_fact_gas_station_sales(spark, path):
    customer_df = read_silver_main(spark, "gasstation/customer").select(
        col("customerid").alias("CustomerID"),
        col("customername").alias("CustomerName"),
        col("email").alias("CustomerEmail")
    )

    employee_df = read_silver_main(spark, "gasstation/employee").select(
        col("employeeid").alias("EmployeeID"),
        col("employeename").alias("EmployeeName"),
        col("gasstationid").alias("GasStationID")
    )

    gas_station_df = read_silver_main(spark, "gasstation/gasstation").select(
        col("gasstationid").alias("GasStationID"),
        col("gasstationname").alias("GasStationName"),
        col("address").alias("GasStationAddress")
    )

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
        col("invoicedetailid").alias("SalesKey"),
        col("invoiceid").alias("InvoiceID"),
        col("productid").alias("ProductKey"),
        col("quantitysold").alias("QuantitySold"),
        col("sellingprice").alias("UnitPrice"),
        col("totalprice").alias("DetailTotalAmount")
    )

    product_df = read_silver_main(spark, "gasstation/product").select(
        col("productid").alias("ProductKey"),
        col("productname").alias("ProductName"),
        col("unitprice").alias("ProductUnitPrice")
    )

    sales_fact_df = invoice_df.join(
        invoice_detail_df, "InvoiceID", "inner"
    ).join(
        customer_df, "CustomerID", "inner"
    ).join(
        employee_df, "EmployeeID", "inner"
    ).join(
        gas_station_df, "GasStationID", "inner"
    ).join(
        product_df, "ProductKey", "inner"
    ).withColumn(
        "TaxAmount", col("DetailTotalAmount") * lit(0.1)
    ).withColumn(
        "Discount", lit(0.0)
    ).withColumn(
        "SalesChannel", lit("InStore")
    ).withColumn(
        "PromotionID", lit(None).cast("string")
    )

    final_fact_gas_station_sales = sales_fact_df.select(
        col("SalesKey"),
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
        col("SalesChannel"),
        col("PromotionID")
    )

    write_to_warehouse(final_fact_gas_station_sales,
                       "fact_gas_station_sales", path, recordkey="SalesKey", precombine="TimeKey")


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/fact_gas_station_sales/"
    spark = create_spark_session(
        "FactGasStationSales", lakefs_user["username"], lakefs_user["password"])
    create_fact_gas_station_sales(spark, path)
    spark.stop()
