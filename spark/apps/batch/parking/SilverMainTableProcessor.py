"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, current_timestamp
from typing import Tuple, Optional
from common import get_redis_client, get_lakefs, create_spark_session
from common.table.table_config import TableConfig
import json

lakefs_user = get_lakefs()


class MainSyncProcessor:
    def __init__(self, spark: SparkSession, table_name: str):
        self.spark = spark
        self.table_name = table_name
        self.table_config = TableConfig.get_table_config(table_name)

    def read_hudi_table(self, path: str) -> DataFrame:
        try:
            return self.spark.read \
                .format("hudi") \
                .load(path)
        except Exception as e:
            print(f"No existing Hudi table found at {path}")
            return None

    def get_base_columns(self, df: DataFrame) -> list:
        return [col for col in df.columns if not col.startswith('_hoodie_') and col != 'last_update']

    def detect_changes(self, staging_df: DataFrame, main_df: Optional[DataFrame]) -> DataFrame:
        staging_df.createOrReplaceTempView("staging_table")

        if main_df is not None and main_df.count() > 0:
            base_columns = self.get_base_columns(main_df)
            main_df.createOrReplaceTempView("main_table")

            compare_conditions = " OR ".join([
                f"s.{col} != m.{col}" for col in self.table_config['compare_columns']
            ])

            updates_query = f"""
                SELECT {', '.join(f's.{col}' for col in base_columns)},
                       'UPDATE' as change_type
                FROM staging_table s
                JOIN main_table m
                ON s.{self.table_config['record_key']} = m.{self.table_config['record_key']}
                WHERE {compare_conditions}
            """
            updates_df = self.spark.sql(updates_query)

            deletes_query = f"""
                SELECT {', '.join(f'm.{col}' for col in base_columns)},
                       'DELETE' as change_type
                FROM main_table m
                LEFT JOIN staging_table s
                ON m.{self.table_config['record_key']} = s.{self.table_config['record_key']}
                WHERE s.{self.table_config['record_key']} IS NULL
            """
            deletes_df = self.spark.sql(deletes_query)

            inserts_query = f"""
                SELECT {', '.join(f's.{col}' for col in base_columns)},
                       'INSERT' as change_type
                FROM staging_table s
                LEFT JOIN main_table m
                ON s.{self.table_config['record_key']} = m.{self.table_config['record_key']}
                WHERE m.{self.table_config['record_key']} IS NULL
            """
            inserts_df = self.spark.sql(inserts_query)

            return inserts_df.union(updates_df).union(deletes_df) \
                .withColumn("last_update", current_timestamp())
        else:
            return staging_df.withColumn("change_type", lit("INSERT")) \
                .withColumn("last_update", current_timestamp())

    def write_to_hudi(self, df: DataFrame, path: str, operation: str = 'upsert') -> None:
        if df.count() > 0:
            if operation not in ['upsert', 'delete']:
                raise ValueError(f"Invalid operation: {operation}")
            print(f"Writing to Hudi table at \
              {path} with operation {operation}")

            if operation == 'delete':
                main_df = self.read_hudi_table(path)
                if main_df is not None and main_df.count() > 0:
                    metadata_cols = [
                        col for col in main_df.columns if col.startswith('_hoodie_')]
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
        self.spark = create_spark_session(
            lakefs_user["username"], lakefs_user["password"], f"SilverMainParking_{table_name}_processor")
        self.data_processor = MainSyncProcessor(self.spark, table_name)

    def check_changes(self) -> None:
        try:
            staging_df = self.data_processor.read_hudi_table(
                f"s3a://silver/staging_parking/parking/{self.table_name}")
            main_df = self.data_processor.read_hudi_table(
                f"s3a://silver/main/parking/{self.table_name}")

            if staging_df is not None:
                changes_df = self.data_processor.detect_changes(
                    staging_df, main_df)

                if changes_df.count() > 0:
                    redis_client = get_redis_client()
                    sync_branch = redis_client.lpop(
                        f"sync_branch_parking_{self.table_name}").decode('utf-8')
                    print(f"Sync branch: {sync_branch}")

                    path = f"s3a://silver/{sync_branch}/parking/parking_\
                      {self.table_name}_sync".replace(" ", "")
                    self.data_processor.write_to_hudi(changes_df, path)
        finally:
            self.spark.stop()

    def sync_to_main(self) -> None:
        try:
            redis_client = get_redis_client()
            config_str = redis_client.lpop(
                f"parking_sync_{self.table_name}")
            config = json.loads(config_str)

            path = f"s3a://silver/\
              {config['sync_branch']}/parking/parking_{self.table_name}_sync".replace(" ", "")

            sync_df = self.data_processor.read_hudi_table(path)

            if sync_df is not None and sync_df.count() > 0:
                inserts_updates_df = sync_df.filter(
                    "change_type IN ('INSERT', 'UPDATE')")
                if inserts_updates_df.count() > 0:
                    self.data_processor.write_to_hudi(
                        inserts_updates_df.drop("change_type"),
                        f"s3a://silver/main/parking/{self.table_name}"
                    )

                deletes_df = sync_df.filter("change_type = 'DELETE'")
                if deletes_df.count() > 0:
                    self.data_processor.write_to_hudi(
                        deletes_df.drop("change_type"),
                        f"s3a://silver/main/parking/{self.table_name}",
                        operation='delete'
                    )
        finally:
            self.spark.stop()
