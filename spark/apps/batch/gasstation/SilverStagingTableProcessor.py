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
        """Read data from PostgreSQL table"""
        return self.spark.read \
            .format("jdbc") \
            .option("url", self.postgres_config["url"]) \
            .option("dbtable", f"gasstation.{self.table_name}") \
            .option("user", self.postgres_config["user"]) \
            .option("password", self.postgres_config["password"]) \
            .option("driver", self.postgres_config["driver"]) \
            .load()

    def read_hudi_table(self, path: str) -> DataFrame:
        """Read data from Hudi table"""
        try:
            return self.spark.read \
                .format("hudi") \
                .load(path)
        except Exception as e:
            print(f"Error:No existing Hudi table found at {path}: {str(e)}")
            postgres_df = self.read_postgres_table()
            return self.spark.createDataFrame([], postgres_df.schema)

    def detect_changes(self, postgres_df: DataFrame, hudi_df: Optional[DataFrame]) -> DataFrame:
        """Detect changes between PostgreSQL and Hudi data"""
        postgres_df.createOrReplaceTempView("postgres_table")

        if hudi_df is not None:
            hudi_df.createOrReplaceTempView("hudi_table")

            # Build comparison conditions for UPDATE
            compare_conditions = " OR ".join([
                f"p.{col} != h.{col}" for col in self.table_config['compare_columns']
            ])

            # Find updates
            updates_df = self.spark.sql(f"""
                SELECT p.*, 'UPDATE' as change_type
                FROM postgres_table p
                JOIN hudi_table h
                ON p.{self.table_config['record_key']} = h.{self.table_config['record_key']}
                WHERE {compare_conditions}
            """)

            # Find deletes
            deletes_df = self.spark.sql(f"""
                SELECT h.*, 'DELETE' as change_type
                FROM hudi_table h
                LEFT JOIN postgres_table p
                ON h.{self.table_config['record_key']} = p.{self.table_config['record_key']}
                WHERE p.{self.table_config['record_key']} IS NULL
            """)

            # Find inserts
            inserts_df = self.spark.sql(f"""
                SELECT p.*, 'INSERT' as change_type
                FROM postgres_table p
                LEFT JOIN hudi_table h
                ON p.{self.table_config['record_key']} = h.{self.table_config['record_key']}
                WHERE h.{self.table_config['record_key']} IS NULL
            """)

            return inserts_df.union(updates_df).union(deletes_df)
        else:
            # If no Hudi table exists, all records are inserts
            return postgres_df.withColumn("change_type", lit("INSERT"))

    def write_to_hudi(self, df: DataFrame, path: str, operation: str = 'upsert') -> None:
        """Write DataFrame to Hudi table"""
        if df.count() > 0:
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
                                          f"gasstation_{table_name}_processor")
        self.data_processor = DataProcessor(self.spark, table_name)

    def check_changes(self) -> None:
        """Check for changes between PostgreSQL and Hudi tables"""
        try:
            # Read source data
            postgres_df = self.data_processor.read_postgres_table()
            hudi_df = self.data_processor.read_hudi_table(
                f"s3a://silver/staging_gasstation/gasstation_{self.table_name}")

            # Detect changes
            changes_df = self.data_processor.detect_changes(
                postgres_df, hudi_df)

            if changes_df.count() > 0:
                # Get modified branch name from Redis
                redis_client = get_redis_client()
                modified_branch = redis_client.lpop(
                    "modified_branch").decode('utf-8')

                # Write changes to modified branch
                self.data_processor.write_to_hudi(
                    changes_df,
                    f"s3a://silver/{modified_branch}/gasstation_{self.table_name}_modified"
                )
        finally:
            self.spark.stop()

    def update_hudi(self) -> None:
        """Update Hudi tables based on detected changes"""
        try:
            # Get configuration from Redis
            redis_client = get_redis_client()
            config_str = redis_client.lpop(
                f"gasstation_modified_{self.table_name}")
            config = json.loads(config_str)

            # Read modified data
            modified_df = self.data_processor.read_hudi_table(
                f"s3a://silver/\
                  {config['modified_branch']}/gasstation_{self.table_name}_modified"
            )

            if modified_df is not None and modified_df.count() > 0:
                # Process inserts and updates
                inserts_updates_df = modified_df.filter(
                    "change_type IN ('INSERT', 'UPDATE')")
                if inserts_updates_df.count() > 0:
                    self.data_processor.write_to_hudi(
                        inserts_updates_df.drop("change_type"),
                        f"s3a://silver/staging_gasstation/gasstation_\
                          {self.table_name}"
                    )

                # Process deletes
                deletes_df = modified_df.filter("change_type = 'DELETE'")
                if deletes_df.count() > 0:
                    self.data_processor.write_to_hudi(
                        deletes_df.drop("change_type"),
                        f"s3a://silver/staging_gasstation/gasstation_\
                          {self.table_name}",
                        operation='delete'
                    )
        finally:
            self.spark.stop()
