"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from typing import Dict, Optional
from lakefs import Repository
from common import get_redis_client, get_lakefs_client, get_lakefs, create_spark_session
import json
lakefs_user = get_lakefs()

storagetank_table_config = {
    'record_key': 'tankid',
    'partition_fields': [],
    'compare_columns': ['gasstationid', 'tankname', 'capacity', 'materialtype', 'currentquantity']
}


def get_schema():
    return StructType([
        StructField("tankid", IntegerType(), False),
        StructField("gasstationid", IntegerType(), False),
        StructField("tankname", StringType(), False),
        StructField("capacity", IntegerType(), False),
        StructField("materialtype", StringType(), False),
        StructField("currentquantity", IntegerType(), False),
        StructField("motorbikespaces", IntegerType(), False),
        StructField("last_update", TimestampType(), True)
    ])


schema = get_schema()


def get_hudi_options(table_name: str, operation: str = 'upsert') -> Dict[str, str]:
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'tankid',
        'hoodie.datasource.write.precombine.field': 'last_update',
        'hoodie.datasource.write.operation': operation,
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }


def read_hudi_table(spark: SparkSession, path: str) -> DataFrame:
    try:
        return spark.read \
            .format("hudi") \
            .load(path)
    except Exception as e:
        print(f"No existing Hudi table found at {path}")
        return spark.createDataFrame([], schema)


def write_to_hudi(table_name: str, spark: SparkSession, df: DataFrame, path: str, operation: str = 'upsert') -> None:
    if df.count() > 0:
        if operation not in ['upsert', 'delete']:
            raise ValueError(f"Invalid operation: {operation}")
        print(f"Writing to Hudi table at \
          {path} with operation {operation}")

        if operation == 'delete':
            hudi_df = read_hudi_table(spark, path)
            if hudi_df is not None and hudi_df.count() > 0:
                metadata_cols = [
                    col for col in hudi_df.columns if col.startswith('_hoodie_')]
                for col in metadata_cols:
                    df = df.withColumn(col, lit(None))
        df.write \
            .format("hudi") \
            .options(**get_hudi_options(table_name, operation)) \
            .mode("append" if operation == 'upsert' else "overwrite") \
            .save(path)


def get_base_columns(df: DataFrame) -> list:
    return [col for col in df.columns if not col.startswith('_hoodie_') and col != 'last_update']


def detect_changes(spark: SparkSession, staging_df: DataFrame, main_df: Optional[DataFrame]) -> DataFrame:
    staging_cols = get_base_columns(staging_df)
    staging_df_cleaned = staging_df.select(staging_cols)

    # staging_df_cleaned.createOrReplaceTempView("staging_table")
    if main_df is not None and main_df.count() > 0:
        base_columns = get_base_columns(main_df)

        # main_df.createOrReplaceTempView("main_table")
        # staging_df_cleaned.show()
        # staging_df_cleaned.createOrReplaceTempView("staging_table")
        # spark.sql("show tables;").show()
        # # Build comparison conditions
        # compare_conditions = " OR ".join([
        #     f"s.{col} != m.{col}" for col in parkinglot_table_config['compare_columns']
        # ])

        # updates_query = f"""
        #     SELECT {', '.join(f's.{col}' for col in base_columns)},
        #             'UPDATE' as change_type
        #     FROM staging_table s
        #     JOIN main_table m
        #     ON s.{parkinglot_table_config['record_key']} = m.{parkinglot_table_config['record_key']}
        #     WHERE {compare_conditions}
        # """
        # updates_df = spark.sql(updates_query)

        # deletes_query = f"""
        #     SELECT {', '.join(f'm.{col}' for col in base_columns)},
        #             'DELETE' as change_type
        #     FROM main_table m
        #     LEFT JOIN staging_table s
        #     ON m.{parkinglot_table_config['record_key']} = s.{parkinglot_table_config['record_key']}
        #     WHERE s.{parkinglot_table_config['record_key']} IS NULL
        # """
        # deletes_df = spark.sql(deletes_query)

        # # Find inserts
        # inserts_query = f"""
        #     SELECT {', '.join(f's.{col}' for col in base_columns)},
        #             'INSERT' as change_type
        #     FROM staging_table s
        #     LEFT JOIN main_table m
        #     ON s.{parkinglot_table_config['record_key']} = m.{parkinglot_table_config['record_key']}
        #     WHERE m.{parkinglot_table_config['record_key']} IS NULL
        # """
        # inserts_df = spark.sql(inserts_query)
        staging_df_renamed = staging_df_cleaned.toDF(
            *[f"staging_{col}" for col in staging_df_cleaned.columns])
        main_df_renamed = main_df.toDF(
            *[f"main_{col}" for col in main_df.columns])
        updates_df = staging_df_renamed.join(main_df_renamed,
                                             staging_df_renamed[f"staging_{storagetank_table_config['record_key']}"] ==
                                             main_df_renamed[f"main_\
                                               {storagetank_table_config['record_key']}".replace(" ", "")],
                                             'inner') \
            .filter(" OR ".join([
                f"staging_{col} != main_{col}"
                for col in storagetank_table_config['compare_columns']
            ])) \
            .select([f"staging_{col}" for col in base_columns]) \
            .toDF(*base_columns) \
            .withColumn("change_type", lit("UPDATE"))

        deletes_df = main_df_renamed.join(staging_df_renamed,
                                          main_df_renamed[f"main_{storagetank_table_config['record_key']}"] ==
                                          staging_df_renamed[f"staging_\
                                            {storagetank_table_config['record_key']}".replace(" ", "")],
                                          'leftanti') \
            .select([f"main_{col}" for col in base_columns]) \
            .toDF(*base_columns) \
            .withColumn("change_type", lit("DELETE"))

        inserts_df = staging_df_renamed.join(main_df_renamed,
                                             staging_df_renamed[f"staging_{storagetank_table_config['record_key']}"] ==
                                             main_df_renamed[f"main_\
                                               {storagetank_table_config['record_key']}".replace(" ", "")],
                                             'leftanti') \
            .select([f"staging_{col}" for col in base_columns]) \
            .toDF(*base_columns) \
            .withColumn("change_type", lit("INSERT"))
        return inserts_df.union(updates_df).union(deletes_df) \
            .withColumn("last_update", current_timestamp())
    else:
        return staging_df_cleaned.withColumn("change_type", lit("INSERT")) \
            .withColumn("last_update", current_timestamp())


def write_to_hudi(self, df: DataFrame, path: str, operation: str = 'upsert') -> None:
    """Write DataFrame to Hudi table"""
    if df.count() > 0:
        if operation not in ['upsert', 'delete']:
            raise ValueError(f"Invalid operation: {operation}")
        print(f"Writing to Hudi table at \
          {path} with operation {operation}")

        if operation == 'delete':
            hudi_df = self.read_hudi_table(path)
            if hudi_df is not None and hudi_df.count() > 0:
                metadata_cols = [
                    col for col in hudi_df.columns if col.startswith('_hoodie_')]
                for col in metadata_cols:
                    df = df.withColumn(col, lit(None))

        df.write \
            .format("hudi") \
            .options(**get_hudi_options(self.table_name, operation)) \
            .mode("append" if operation == 'upsert' else "overwrite") \
            .save(path)


def process_batch(df, epoch_id, spark_session):
    try:
        client = get_lakefs_client()
        redis_client = get_redis_client()
        repo = Repository("silver", client=client)

        # Extract relevant data from Debezium JSON
        # parsed_df = df.select(
        #     from_json(col("value").cast("string"), "struct<after:struct<parkinglotid:int,name:string,location:string,totalspaces:int,availablespaces:int,carspaces:int,motorbikespaces:int,bicyclespaces:int,type:string,hourlyrate:decimal(10,2)>>").alias("parsed_value")
        # ).select(
        #     col("parsed_value.after.*"),
        # )
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema=StructType([
                StructField("payload", StructType([
                    StructField("after", schema)
                ]))
            ])).alias("parsed_payload")
        ).select(
            col("parsed_payload.payload.after.*")
        )
        parsed_df = parsed_df.select(
            "tankid", "gasstationid", "tankname", "capacity",
            "materialtype", "currentquantity", "motorbikespaces"
        )

        hudi_df = read_hudi_table(spark_session,
                                  f"s3a://silver/staging_gasstation/gasstation/storagetank")

        changes_df = detect_changes(spark_session, parsed_df, hudi_df)
        changes_df.show()
        # Generate branch name based on current timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        branch_name = f"storagetank_{timestamp}_modified"

        branch_exists = False
        for branch in repo.branches():
            if branch.id == branch_name:
                branch_exists = True
                break

        if not branch_exists:
            parking_branch = repo.branch(branch_name).create(
                source_reference="staging_gasstation")
            print("New branch created:", branch_name,
                  "with commit ID:", parking_branch.get_commit().id)
        else:
            parking_branch = repo.branch(branch_name)

        if changes_df.count() > 0:

            write_to_hudi("gasstation_storagetank", spark_session, changes_df,
                          f"s3a://silver/{branch_name}/gasstation/storagetank_modified".replace(" ", ""))

            config = {
                'table_name': 'storagetank',
                'modified_branch': branch_name,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            print("===========================================================")
            print(config)
            print("===========================================================")
            redis_client.rpush(
                "gasstation_modified_storagetank", json.dumps(config))

    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")
        raise e


def process_storagetank_stream():
    spark = create_spark_session(
        lakefs_user["username"],
        lakefs_user["password"],
        "StoragetankToStagingSilverLayer"
    )

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "storagetank") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    checkpoint_location = "file:///opt/spark-data/checkpoint_storagetank_silver"

    query = df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, spark)) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_storagetank_stream()
