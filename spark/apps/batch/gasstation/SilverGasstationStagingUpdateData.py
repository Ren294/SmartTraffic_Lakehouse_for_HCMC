import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from common import get_redis_client, get_lakefs_client, get_lakefs, create_spark_session
import json


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Update Hudi tables with modified gas station data')
    parser.add_argument('--table',
                        required=True,
                        help='Table name to process')
    parser.add_argument('--staging-branch',
                        default='staging_gasstation',
                        help='Name of the staging branch (default: staging_gasstation)')
    parser.add_argument('--base-path',
                        default='s3a://silver',
                        help='Base path for Hudi tables (default: s3a://silver)')
    return parser.parse_args()


def get_hudi_options(table_name):
    """Get Hudi write options for a table"""
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': ','.join(get_primary_keys(table_name.replace('gasstation_', ''))),
        'hoodie.datasource.write.precombine.field': 'modified_timestamp',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }


def update_hudi_table(spark, config):
    """Update Hudi table with modified records"""
    # Get processing information from Redis
    redis_client = get_redis_client()
    config_str = redis_client.lpop(f"gasstation_modified_{config.table}")
    if not config_str:
        raise Exception(f"No modification data found for table {config.table}")

    redis_config = json.loads(config_str)
    modified_branch = redis_config['modified_branch']

    # Read modified records
    modified_df = spark.read.format("hudi") \
        .load(f"{config.base_path}/{modified_branch}/gasstation_modified_{config.table}/")

    # Process records based on change_type
    inserts = modified_df.filter(col("change_type") == "INSERT") \
        .drop("change_type", "modified_timestamp")

    updates = modified_df.filter(col("change_type") == "UPDATE") \
        .drop("change_type", "modified_timestamp")

    deletes = modified_df.filter(col("change_type") == "DELETE") \
        .drop("change_type", "modified_timestamp")

    # Read current Hudi table
    try:
        current_df = spark.read.format("hudi") \
            .load(f"{config.base_path}/{config.staging_branch}/gasstation_{config.table}/")
    except Exception as e:
        print(f"No existing Hudi table found. Creating new table for {
              config.table}")
        current_df = None

    # Handle table updates
    pk_columns = get_primary_keys(config.table)
    result_df = None

    if current_df is not None:
        # Remove deleted records
        if deletes.count() > 0:
            current_df = current_df.join(
                deletes.select(*pk_columns),
                pk_columns,
                "left_anti"
            )

        # Combine with new/updated records
        result_df = current_df.unionAll(inserts).unionAll(updates)
    else:
        # If no existing table, combine inserts and updates
        result_df = inserts.unionAll(updates)

    # Write back to Hudi
    if result_df is not None:
        result_df.write \
            .format("hudi") \
            .options(**get_hudi_options(f"gasstation_{config.table}")) \
            .mode("overwrite") \
            .save(f"{config.base_path}/{config.staging_branch}/gasstation_{config.table}/")


def main():
    """Main entry point"""
    args = parse_arguments()

    try:
        lakefs_user = get_lakefs()
        spark = create_spark_session(
            lakefs_user["username"],
            lakefs_user["password"]
        )

        update_hudi_table(spark, args)
        spark.stop()

    except Exception as e:
        print(f"Error updating table {args.table}: {str(e)}")
        if 'spark' in locals():
            spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
