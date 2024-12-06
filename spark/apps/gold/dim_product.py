"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from common import read_silver_main, create_spark_session, get_lakefs, write_to_warehouse, create_table_warehouse


def create_dim_product(spark, path):
    product_df = read_silver_main(spark, "gasstation/product")\
        .select(
            col("productid").alias("ProductID"),
            col("productname").alias("ProductName"),
            col("unitprice").alias("UnitPrice"),
            col("producttype").alias("ProductType"),
            col("supplier").alias("Supplier"),
            col("stockquantity").alias("StockQuantity"),
            col("last_update").alias("LastUpdate")
    )
    final_dim_product = product_df\
        .withColumn("TaxRate",
                    when(col("ProductType") == "Fuel", lit(0.1))
                    .when(col("ProductType") == "Lubricant", lit(0.08))
                    .otherwise(lit(0.05)))\
        .withColumn("DiscountEligibility",
                    when(col("StockQuantity") > 100, lit(True))
                    .otherwise(lit(False)))\
        .withColumn("ProductKey", monotonically_increasing_id())

    write_to_warehouse(
        final_dim_product,
        "dim_product",
        path,
        recordkey="ProductKey",
        precombine="LastUpdate"
    )


if __name__ == "__main__":
    lakefs_user = get_lakefs()
    path = "s3a://gold/main/warehouse/dim_product/"
    spark = create_spark_session(
        "DimProducts", lakefs_user["username"], lakefs_user["password"])
    create_dim_product(spark, path)
    spark.stop()
