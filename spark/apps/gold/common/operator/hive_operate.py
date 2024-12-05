def create_table_warehouse(spark, tablename, path, schema="default"):
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {schema}.{tablename}
      USING hudi
      LOCATION '{path}'
    """)
