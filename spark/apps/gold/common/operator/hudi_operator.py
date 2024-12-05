def read_silver_main(spark, path_table):
    s3_path = f"s3a://silver/main/{path_table}"
    df = spark.read.format("hudi").load(s3_path)
    return df


def write_to_warehouse(df, table_name, table_path, partitionpath, operation="upsert", recordkey="record_id", precombine="timestamp"):
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": recordkey,
        "hoodie.datasource.write.table.name": table_name,
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.datasource.write.partitionpath.field": partitionpath,
    }

    s3_path = f"{table_path}"

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(s3_path)
