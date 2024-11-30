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
    """Commit changes to staging branch"""
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    staging_branch = repo.branch("staging_gasstation")

    commit_message = "Updated storagetank data with latest changes"
    try:
        commit = staging_branch.commit(message=commit_message)
        return {'status': 'success', 'commit_id': commit.get_commit().id}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


# Create DAG
dag = DAG(
    'Silver_to_Staging_Storagetank_Merge_DAG',
    default_args=default_args,
    description='Process storagetank data from Kafka to LakeFS',
    schedule_interval='@hourly',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

# Create SSH Hook
ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

# Define tasks
start_dag = DummyOperator(task_id='start_dag', dag=dag)


update_hudi_task = SSHOperator(
    task_id='update_hudi',
    ssh_hook=ssh_hook,
    command=spark_submit(
        "batch/gasstation/SilverUpdateStaging_storagetank.py"),
    dag=dag
)

commit_task = PythonOperator(
    task_id='commit_changes',
    python_callable=commit_changes,
    provide_context=True,
    dag=dag
)

end_dag = DummyOperator(task_id='end_dag', dag=dag)

# Set up task dependencies
start_dag >> update_hudi_task >> commit_task >> end_dag
