import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from common import get_redis_client, get_lakefs_client, get_lakefs, create_spark_session, get_postgres_properties
import json
import sys


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Compare PostgreSQL and Hudi tables for gas station data')
    parser.add_argument('--table',
                        required=True,
                        help='Table name to process')
    parser.add_argument('--modified-branch',
                        required=True,
                        help='Branch name for modified data')
    parser.add_argument('--staging-branch',
                        default='staging_gasstation',
                        help='Name of the staging branch (default: staging_gasstation)')
    parser.add_argument('--base-path',
                        default='s3a://silver',
                        help='Base path for Hudi tables (default: s3a://silver)')
    return parser.parse_args()


def read_postgres_table(spark, table_name):
    """Read data from PostgreSQL table"""
    return spark.read \
        .format("jdbc") \
        .options(**get_postgres_properties()) \
        .option("dbtable", f"gasstation.{table_name}") \
        .load()


def read_hudi_table(spark, base_path, branch, table_name):
    """Read data from Hudi table"""
    try:
        return spark.read.format("hudi") \
            .load(f"{base_path}/{branch}/gasstation_{table_name}/")
    except Exception as e:
        print(f"No existing Hudi table found for {table_name}: {str(e)}")
        return None


def compare_data(spark, config):
    """Compare data between PostgreSQL and Hudi tables"""
    print(f"Starting comparison for table {
          config.table} using branch {config.modified_branch}")

    # Read current data from PostgreSQL
    postgres_df = read_postgres_table(spark, config.table)

    if postgres_df.count() == 0:
        print(f"Warning: No data found in PostgreSQL for table {config.table}")
        return

    # Read data from Hudi
    hudi_df = read_hudi_table(spark, config.base_path,
                              config.staging_branch, config.table)

    # Get primary key columns for the table
    pk_columns = get_primary_keys(config.table)

    # Initialize modified records DataFrame
    if hudi_df is None:
        # If no Hudi table exists, all records are new inserts
        modified_records = postgres_df.withColumn("change_type", lit("INSERT"))
    else:
        # Find inserted records
        inserted = postgres_df.join(
            hudi_df,
            pk_columns,
            "left_anti"
        ).withColumn("change_type", lit("INSERT"))

        # Find deleted records
        deleted = hudi_df.join(
            postgres_df,
            pk_columns,
            "left_anti"
        ).withColumn("change_type", lit("DELETE"))

        # Find updated records
        postgres_common = postgres_df.join(
            hudi_df.select(*pk_columns),
            pk_columns,
            "inner"
        )

        updated = []
        for col_name in postgres_df.columns:
            if col_name not in pk_columns:
                updated_records = postgres_common.join(
                    hudi_df,
                    pk_columns
                ).where(col(f"postgres_df.{col_name}") != col(f"hudi_df.{col_name}"))

                if updated_records.count() > 0:
                    updated.append(
                        updated_records.select(
                            postgres_df["*"],
                            lit("UPDATE").alias("change_type")
                        )
                    )

        # Combine all changes
        if updated:
            updated_df = updated[0]
            for df in updated[1:]:
                updated_df = updated_df.unionAll(df)

            modified_records = inserted.unionAll(deleted).unionAll(updated_df)
        else:
            modified_records = inserted.unionAll(deleted)

    # Add timestamp column
    modified_records = modified_records.withColumn(
        "modified_timestamp",
        current_timestamp()
    )

    # Write modified records to the modified branch
    print(f"Writing modified records to branch {config.modified_branch}")
    modified_records.write \
        .format("hudi") \
        .options(**get_hudi_options(f"gasstation_modified_{config.table}")) \
        .mode("overwrite") \
        .save(f"{config.base_path}/{config.modified_branch}/gasstation_modified_{config.table}/")


def main():
    """Main entry point"""
    args = parse_arguments()

    try:
        lakefs_user = get_lakefs()
        spark = create_spark_session(
            lakefs_user["username"],
            lakefs_user["password"]
        )

        compare_data(spark, args)
        spark.stop()

    except Exception as e:
        print(f"Error processing table {args.table}: {str(e)}")
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
