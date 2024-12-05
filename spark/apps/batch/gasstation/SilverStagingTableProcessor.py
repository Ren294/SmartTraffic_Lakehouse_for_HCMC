"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp
from typing import Tuple, Optional
from common import get_redis_client, get_lakefs, get_postgres_properties, create_spark_session
from common.table.table_config import TableConfig
import json

lakefs_user = get_lakefs()


class DataProcessor:
    def __init__(self, spark: SparkSession, table_name: str):
        self.spark = spark
        self.table_name = table_name
        self.table_config = TableConfig.get_table_config(table_name)
        self.postgres_config = get_postgres_properties()

    def read_postgres_table(self) -> DataFrame:
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.postgres_config["url"]) \
            .option("dbtable", f"gasstation.{self.table_name}") \
            .option("user", self.postgres_config["user"]) \
            .option("password", self.postgres_config["password"]) \
            .option("driver", self.postgres_config["driver"]) \
            .load()

    def read_hudi_table(self, path: str) -> DataFrame:
        try:
            return self.spark.read \
                .format("hudi") \
                .load(path)
        except Exception as e:
            print(f"No existing Hudi table found at {path}")
            postgres_df = self.read_postgres_table()
            return self.spark.createDataFrame([], postgres_df.schema)

    def get_base_columns(self, df: DataFrame) -> list:
        return [col for col in df.columns if not col.startswith('_hoodie_') and col != 'last_update']

    def detect_changes(self, postgres_df: DataFrame, hudi_df: Optional[DataFrame]) -> DataFrame:
        postgres_df.createOrReplaceTempView("postgres_table")

        if hudi_df is not None and hudi_df.count() > 0:
            base_columns = self.get_base_columns(hudi_df)
            hudi_df.createOrReplaceTempView("hudi_table")

            compare_conditions = " OR ".join([
                f"p.{col} != h.{col}" for col in self.table_config['compare_columns']
            ])

            updates_query = f"""
                SELECT {', '.join(f'p.{col}' for col in base_columns)},
                       'UPDATE' as change_type
                FROM postgres_table p
                JOIN hudi_table h
                ON p.{self.table_config['record_key']} = h.{self.table_config['record_key']}
                WHERE {compare_conditions}
            """
            updates_df = self.spark.sql(updates_query)

            deletes_query = f"""
                SELECT {', '.join(f'h.{col}' for col in base_columns)},
                       'DELETE' as change_type
                FROM hudi_table h
                LEFT JOIN postgres_table p
                ON h.{self.table_config['record_key']} = p.{self.table_config['record_key']}
                WHERE p.{self.table_config['record_key']} IS NULL
            """
            deletes_df = self.spark.sql(deletes_query)

            inserts_query = f"""
                SELECT {', '.join(f'p.{col}' for col in base_columns)},
                       'INSERT' as change_type
                FROM postgres_table p
                LEFT JOIN hudi_table h
                ON p.{self.table_config['record_key']} = h.{self.table_config['record_key']}
                WHERE h.{self.table_config['record_key']} IS NULL
            """
            inserts_df = self.spark.sql(inserts_query)

            return inserts_df.union(updates_df).union(deletes_df) \
                .withColumn("last_update", current_timestamp())
        else:
            return postgres_df.withColumn("change_type", lit("INSERT")).withColumn("last_update", current_timestamp())

    def write_to_hudi(self, df: DataFrame, path: str, operation: str = 'upsert') -> None:
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
                .options(**TableConfig.get_hudi_options(self.table_name, operation)) \
                .mode("append" if operation == 'upsert' else "overwrite") \
                .save(path)


class ChangeProcessor:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.spark = create_spark_session(lakefs_user["username"],
                                          lakefs_user["password"],
                                          f"SilverStagingGasstation_{table_name}_processor")
        self.data_processor = DataProcessor(self.spark, table_name)

    def check_changes(self) -> None:
        try:
            postgres_df = self.data_processor.read_postgres_table()
            hudi_df = self.data_processor.read_hudi_table(
                f"s3a://silver/staging_gasstation/gasstation/{self.table_name}")

            changes_df = self.data_processor.detect_changes(
                postgres_df, hudi_df)

            if changes_df.count() > 0:
                redis_client = get_redis_client()
                modified_branch = redis_client.lpop(
                    f"modified_branch_gasstation_{self.table_name}").decode('utf-8')
                print(f"Modified branch: {modified_branch}")
                path = f"s3a://silver/{modified_branch}/gasstation/gasstation_\
                  {self.table_name}_modified".replace(" ", '')
                self.data_processor.write_to_hudi(
                    changes_df,
                    path
                )
        finally:
            self.spark.stop()

    def update_hudi(self) -> None:
        try:
            redis_client = get_redis_client()
            config_str = redis_client.lpop(
                f"gasstation_modified_{self.table_name}")
            config = json.loads(config_str)
            path = f"s3a://silver/{config['modified_branch']}/gasstation/gasstation_\
                  {self.table_name}_modified".replace(" ", '')
            modified_df = self.data_processor.read_hudi_table(path)

            if modified_df is not None and modified_df.count() > 0:
                inserts_updates_df = modified_df.filter(
                    "change_type IN ('INSERT', 'UPDATE')")
                if inserts_updates_df.count() > 0:
                    self.data_processor.write_to_hudi(
                        inserts_updates_df.drop("change_type"),
                        f"s3a://silver/staging_gasstation/gasstation/\
                          {self.table_name}".replace(" ", '')
                    )

                deletes_df = modified_df.filter("change_type = 'DELETE'")
                if deletes_df.count() > 0:
                    self.data_processor.write_to_hudi(
                        deletes_df.drop("change_type"),
                        f"s3a://silver/staging_gasstation/gasstation/\
                          {self.table_name}".replace(" ", ''),
                        operation='delete'
                    )
        finally:
            self.spark.stop()
