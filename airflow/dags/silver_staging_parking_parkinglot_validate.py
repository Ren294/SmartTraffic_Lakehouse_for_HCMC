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


def create_modified_branch(**context):
    """Create a new branch for modified data"""
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    branch_name = f"parking_{timestamp}_modified"

    repo.branch(branch_name).create(source_reference="staging_parking")
    context['task_instance'].xcom_push(
        key='modified_branch', value=branch_name)
    return branch_name


def push_branch_to_redis(**context):
    """Push branch name to Redis"""
    modified_branch = context['task_instance'].xcom_pull(
        task_ids='create_modified_branch',
        key='modified_branch'
    )
    redis_client = get_redis_client()
    redis_key = "modified_branch_parking_parkinglot"

    redis_client.delete(redis_key)
    redis_client.rpush(redis_key, modified_branch)
    return {'redis_key': redis_key, 'branch': modified_branch}


def push_to_redis_queue(**context):
    """Push processing information to Redis queue"""
    modified_branch = context['task_instance'].xcom_pull(
        task_ids='create_modified_branch',
        key='modified_branch'
    )

    config = {
        'table_name': 'parkinglot',
        'modified_branch': modified_branch,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    redis_client = get_redis_client()
    redis_client.rpush("parking_modified_parkinglot", json.dumps(config))
    return config


def commit_changes(**context):
    """Commit changes to staging branch"""
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    staging_branch = repo.branch("staging_parking")

    commit_message = "Updated parkinglot data with latest changes"
    try:
        commit = staging_branch.commit(message=commit_message)
        return {'status': 'success', 'commit_id': commit.get_commit().id}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


# Create DAG
dag = DAG(
    'Silver_Staging_Parking_Parkinglot_DAG',
    default_args=default_args,
    description='Process parkinglot data from Kafka to LakeFS',
    schedule_interval='@hourly',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

# Create SSH Hook
ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

# Define tasks
start_dag = DummyOperator(task_id='start_dag', dag=dag)

create_branch_task = PythonOperator(
    task_id='create_modified_branch',
    python_callable=create_modified_branch,
    provide_context=True,
    dag=dag
)

push_branch_task = PythonOperator(
    task_id='push_branch_redis',
    python_callable=push_branch_to_redis,
    provide_context=True,
    dag=dag
)

check_changes_task = SSHOperator(
    task_id='check_changes',
    ssh_hook=ssh_hook,
    command=spark_submit(
        "batch/parking/SilverCheckChangesStaging_parkinglot.py"),
    dag=dag
)

push_queue_task = PythonOperator(
    task_id='push_queue',
    python_callable=push_to_redis_queue,
    provide_context=True,
    dag=dag
)

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

end_dag = DummyOperator(task_id='end_dag', dag=dag)

# Set up task dependencies
start_dag >> create_branch_task >> push_branch_task >> check_changes_task >> \
    push_queue_task >> update_hudi_task >> commit_task >> end_dag
