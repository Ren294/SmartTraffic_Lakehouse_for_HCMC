from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import json
import redis
from lakefs import Repository
from connection import get_redis_client, get_lakefs_client

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def get_merge_info(**context):
    """Get merge information from Redis queue"""
    redis_client = get_redis_client()
    merge_info = redis_client.lpop("silver_merge_queue_weather")

    if not merge_info:
        raise Exception("No merge request found in queue")

    merge_data = json.loads(merge_info)
    context['task_instance'].xcom_push(key='merge_info', value=merge_data)
    return merge_data


def prepare_spark_config(**context):
    """Prepare configuration for Spark job"""
    merge_info = context['task_instance'].xcom_pull(
        task_ids='get_merge_info', key='merge_info')

    config = {
        'source_branch': merge_info['branch'],
        'target_branch': 'staging_weather',
        'date': merge_info['date']
    }
    redis_client = get_redis_client()
    redis_client.rpush("silver_merge_queue_weather_pre_checked",
                       json.dumps(config))
    context['task_instance'].xcom_push(key='spark_config', value=config)
    return config


def commit_changes(**context):
    """Commit changes to staging branch after successful merge"""
    merge_info = context['task_instance'].xcom_pull(
        task_ids='get_merge_info', key='merge_info')

    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    staging_branch = repo.branch("staging_weather")

    metadata = json.loads(merge_info['metadata'])

    commit_message = f"Merged weather data from \
      {merge_info['branch']} for date {merge_info['date']}"

    try:
        commit = staging_branch.commit(
            message=commit_message,
            metadata=metadata
        )
        config = {
            'source_branch': 'staging_weather',
            'target_branch': 'main',
            'date': merge_info['date']
        }
        redis_client = get_redis_client()
        redis_client.rpush("main_merge_queue_weather",
                           json.dumps(config))
        return {
            'status': 'success',
            'commit_id': commit.get_commit().id,
            'branch': 'staging_weather'
        }
    except Exception as e:
        raise Exception(f"Failed to commit changes: {str(e)}")


# Create DAG
dag = DAG(
    'Silver_to_Staging_Weather_Merge_DAG',
    default_args=default_args,
    description='Merge weather data from silver branch to staging',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

# Create SSH Hook
ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

# Define tasks
start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

get_merge_info_task = PythonOperator(
    task_id='get_merge_info',
    python_callable=get_merge_info,
    provide_context=True,
    dag=dag
)

prepare_spark_config_task = PythonOperator(
    task_id='prepare_spark_config',
    python_callable=prepare_spark_config,
    provide_context=True,
    dag=dag
)

check_conflicts_task = SSHOperator(
    task_id='check_conflicts',
    ssh_hook=ssh_hook,
    command="""/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:3.2.0,org.apache.hudi:hudi-spark3.2-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026,io.lakefs:hadoop-lakefs-assembly:0.2.4 /opt/spark-apps/streaming/CheckConflictsWeatherSilver.py""",
    dag=dag
)

merge_data_task = SSHOperator(
    task_id='merge_data',
    ssh_hook=ssh_hook,
    command="""/opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.kafka:kafka-clients:3.2.0,org.apache.hudi:hudi-spark3.2-bundle_2.12:0.15.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026,io.lakefs:hadoop-lakefs-assembly:0.2.4 /opt/spark-apps/streaming/MergeStagingWeatherSilver.py""",
    dag=dag
)

commit_changes_task = PythonOperator(
    task_id='commit_changes',
    python_callable=commit_changes,
    provide_context=True,
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

# Define task dependencies
start_dag >> get_merge_info_task >> prepare_spark_config_task >> check_conflicts_task >> merge_data_task >> commit_changes_task >> end_dag
