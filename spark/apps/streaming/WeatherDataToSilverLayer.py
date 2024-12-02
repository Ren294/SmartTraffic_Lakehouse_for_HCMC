"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
import json
from datetime import datetime
from typing import Dict, List, Any
from functools import reduce

from common import get_redis_client, get_lakefs_client, get_lakefs, create_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from lakefs import Repository, Client

lakefs_user = get_lakefs()


def get_schema():
    return StructType([
        StructField("datetime", StringType(), True),
        StructField("datetimeEpoch", LongType(), True),
        StructField("temp", FloatType(), True),
        StructField("feelslike", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("dew", FloatType(), True),
        StructField("precip", FloatType(), True),
        StructField("precipprob", FloatType(), True),
        StructField("snow", FloatType(), True),
        StructField("snowdepth", FloatType(), True),
        StructField("preciptype", StringType(), True),
        StructField("windgust", FloatType(), True),
        StructField("windspeed", FloatType(), True),
        StructField("winddir", FloatType(), True),
        StructField("pressure", FloatType(), True),
        StructField("visibility", FloatType(), True),
        StructField("cloudcover", FloatType(), True),
        StructField("solarradiation", FloatType(), True),
        StructField("solarenergy", FloatType(), True),
        StructField("uvindex", IntegerType(), True),
        StructField("severerisk", IntegerType(), True),
        StructField("conditions", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("source", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("name", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("resolvedAddress", StringType(), True),
        StructField("date", StringType(), True),
        StructField("address", StringType(), True),
        StructField("tzoffset", IntegerType(), True),
        StructField("day_visibility", FloatType(), True),
        StructField("day_cloudcover", FloatType(), True),
        StructField("day_uvindex", IntegerType(), True),
        StructField("day_description", StringType(), True),
        StructField("day_tempmin", FloatType(), True),
        StructField("day_windspeed", FloatType(), True),
        StructField("day_icon", StringType(), True),
        StructField("day_precip", FloatType(), True),
        StructField("day_tempmax", FloatType(), True),
        StructField("day_precipcover", FloatType(), True),
        StructField("day_pressure", FloatType(), True),
        StructField("day_preciptype", StringType(), True),
        StructField("day_humidity", FloatType(), True),
        StructField("day_conditions", StringType(), True),
        StructField("day_feelslike", FloatType(), True),
        StructField("day_dew", FloatType(), True),
        StructField("day_sunrise", StringType(), True),
        StructField("day_sunriseEpoch", LongType(), True),
        StructField("day_feelslikemax", FloatType(), True),
        StructField("day_windgust", FloatType(), True),
        StructField("day_solarenergy", FloatType(), True),
        StructField("day_sunset", StringType(), True),
        StructField("day_snowdepth", FloatType(), True),
        StructField("day_sunsetEpoch", LongType(), True),
        StructField("day_severerisk", IntegerType(), True),
        StructField("day_solarradiation", FloatType(), True),
        StructField("day_precipprob", FloatType(), True),
        StructField("day_temp", FloatType(), True),
        StructField("day_winddir", FloatType(), True),
        StructField("day_moonphase", FloatType(), True),
        StructField("day_feelslikemin", FloatType(), True),
        StructField("day_snow", FloatType(), True)
    ])


def get_hudi_options(table_name: str) -> Dict[str, str]:
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'datetime,date',
        'hoodie.datasource.write.partitionpath.field': 'date',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'datetimeEpoch',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }


def validate_datetime_format(datetime_str):
    return when(regexp_extract(col(datetime_str), r'^([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$', 0) != '', True).otherwise(False)


def get_validation_conditions():
    return [
        col('datetime').isNotNull() & col('date').isNotNull(),
        validate_datetime_format('datetime'),
        (col('humidity').isNull() | (col('humidity') >= 0) & (col('humidity') <= 100)),
        (col('temp').isNull() | (col('temp') >= -50) & (col('temp') <= 60))
    ]


def process_batch(df, epoch_id, spark_session):
    try:
        client = get_lakefs_client()
        redis_client = get_redis_client()
        repo = Repository("silver", client=client)

        validation_conditions = get_validation_conditions()
        combined_condition = validation_conditions[0]
        for condition in validation_conditions[1:]:
            combined_condition = combined_condition & condition
        valid_records = df.filter(combined_condition)
        invalid_records = df.filter(~combined_condition)

        if invalid_records.count() > 0:
            invalid_records.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "broker:29092") \
                .option("topic", "weather_letter_dead_queue") \
                .save()

        for date_group in valid_records.groupBy("date").agg(
            collect_list(struct([col(c) for c in df.columns])).alias("records")
        ).collect():
            date = date_group["date"]
            records = date_group["records"]

            has_day_start = any(r["datetime"] == "00:00:00" for r in records)
            branch_name = f"branch_weather_{date}"

            branch_exists = False

            for branch in repo.branches():
                if branch.id == branch_name:
                    print(f"Branch '{branch_name}' already exists.")
                    branch_exists = True
                    break

            if not branch_exists:
                weather_branch = repo.branch(
                    branch_name).create(source_reference="staging_weather")
                print("New branch created:", branch_name,
                      "with commit ID:", weather_branch.get_commit().id)
            else:
                weather_branch = repo.branch(branch_name)

            schema = get_schema()
            rows = []
            for record in records:
                row = []
                for field in schema.fields:
                    value = record[field.name]
                    if value is None:
                        row.append(None)
                    else:
                        try:
                            if isinstance(field.dataType, StringType):
                                row.append(str(value))
                            elif isinstance(field.dataType, IntegerType):
                                row.append(int(value) if value != '' else None)
                            elif isinstance(field.dataType, LongType):
                                row.append(int(value) if value != '' else None)
                            elif isinstance(field.dataType, FloatType):
                                row.append(
                                    float(value) if value != '' else None)
                            else:
                                row.append(value)
                        except (ValueError, TypeError):
                            row.append(None)
                rows.append(row)

            records_df = spark_session.createDataFrame(rows, schema)

            records_df.write \
                .format("hudi") \
                .options(**get_hudi_options(f"weather_HCMC")) \
                .mode("append") \
                .save(f"s3a://silver/{branch_name}/weather/")

            metadata = {
                "date": date,
                "records_count": str(len(records)),
                "has_day_start": str(has_day_start),
                "has_day_end": str(any(r["datetime"] == "23:00:00" for r in records))
            }

            try:
                commit_response = weather_branch.commit(
                    message=f"Added weather data for {date}",
                    metadata=metadata
                )
                commit_id = commit_response.get_commit().id

                md = {
                    "branch": branch_name,
                    "date": date,
                    "commit_id": commit_id,
                    "metadata": json.dumps(metadata)
                }
                # redis_client.hset(
                #     f"silver_{branch_name}",
                #     mapping={
                #         "date": date,
                #         "commit_id": commit_id,
                #         "metadata": json.dumps(metadata)
                #     }
                # )
                redis_client.rpush("silver_merge_queue_weather",
                                   json.dumps(md))

            except Exception as commit_error:
                print(f"Error committing to branch \
                  {branch_name}: {str(commit_error)}")
                raise commit_error

    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")
        raise e


def process_weather_stream():
    spark = create_spark_session(
        lakefs_user["username"], lakefs_user["password"], "WeatherDataToSilverLayer")
    schema = get_schema()

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "weatherHCMC_out") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = df.select(
        split(col("value").cast("string"), ",").alias("csv_columns")
    ).select(
        *[col("csv_columns").getItem(i).cast(schema[i].dataType).alias(schema[i].name)
          for i in range(len(schema))]
    )
    checkpoint_location = "file:///opt/spark-data/checkpoint_weather_silver"
    query = parsed_df.writeStream \
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, spark)) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    process_weather_stream()
