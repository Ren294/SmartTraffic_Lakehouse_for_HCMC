"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import json
from lakefs import Repository
from connection import get_redis_client, get_lakefs_client, spark_submit

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def commit_changes(**context):
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    staging_branch = repo.branch("staging_parking")

    commit_message = "Updated parkinglot data with latest changes"
    try:
        commit = staging_branch.commit(message=commit_message)
        return {'status': 'success', 'commit_id': commit.get_commit().id}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


dag = DAG(
    'Silver_to_Staging_Parkinglot_Merge_DAG',
    default_args=default_args,
    description='Process parkinglot data from Kafka to LakeFS',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

start_dag = DummyOperator(task_id='start_dag', dag=dag)


update_hudi_task = SSHOperator(
    task_id='update_hudi',
    ssh_hook=ssh_hook,
    command=spark_submit("batch/parking/SilverUpdateStaging_parkinglot.py"),
    dag=dag
)

commit_task = PythonOperator(
    task_id='commit_changes',
    python_callable=commit_changes,
    provide_context=True,
    dag=dag
)
check_spark_connection = SSHOperator(
    task_id='check_connection_task',
    ssh_hook=ssh_hook,
    command='echo "Connection to Spark server successful"',
    dag=dag
)
end_dag = DummyOperator(task_id='end_dag', dag=dag)

start_dag >> check_spark_connection >> update_hudi_task >> commit_task >> end_dag
