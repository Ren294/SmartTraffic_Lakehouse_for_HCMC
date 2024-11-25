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


def create_sync_branch(**context):
    """Create a new branch for syncing data based on current timestamp"""
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    branch_name = f"parking_{timestamp}_sync"

    # Create new branch from main
    repo.branch(branch_name).create(source_reference="main")

    context['task_instance'].xcom_push(key='sync_branch', value=branch_name)
    return branch_name


def push_branch_to_redis(table_name, **context):
    """Push branch names to Redis for table processing"""
    sync_branch = context['task_instance'].xcom_pull(
        task_ids='create_sync_branch', key='sync_branch')

    redis_client = get_redis_client()
    redis_key = f"sync_branch_parking_{table_name}"

    # Clear existing key if any
    redis_client.delete(redis_key)

    # Push new branch name
    redis_client.rpush(redis_key, sync_branch)
    print(f"Pushed branch {sync_branch} to Redis key {redis_key}")

    return {'redis_key': redis_key, 'branch': sync_branch}


def push_to_redis_queue(table_name, **context):
    """Push table information to Redis queue for processing"""
    sync_branch = context['task_instance'].xcom_pull(
        task_ids='create_sync_branch', key='sync_branch')

    config = {
        'table_name': table_name,
        'sync_branch': sync_branch,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    redis_client = get_redis_client()
    redis_client.rpush(f"parking_sync_{table_name}", json.dumps(config))
    return config


def commit_to_main(**context):
    """Commit changes to main branch after successful update"""
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    main_branch = repo.branch("main")

    commit_message = f"Synced latest changes from staging to main"
    try:
        commit = main_branch.commit(message=commit_message)
        return {'status': 'success', 'commit_id': commit.get_commit().id}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


# Create DAG
dag = DAG(
    'Silver_Main_Parking_Sync_DAG',
    default_args=default_args,
    description='Sync parking data from staging to main branch',
    schedule_interval='@daily',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

# Create SSH Hook
ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

# Start task
start_dag = DummyOperator(task_id='start_dag', dag=dag)

# Create sync branch task
create_branch_task = PythonOperator(
    task_id='create_sync_branch',
    python_callable=create_sync_branch,
    provide_context=True,
    dag=dag
)

# List of tables to process
tables = ['owner', 'feedback', 'parkinglot', 'parkingrecord',
          'payment', 'promotion', 'staff', 'vehicle']

# Create tasks for each table
push_branch_tasks = {}
check_tasks = {}
push_queue_tasks = {}
sync_tasks = {}

for table in tables:
    # Push branch to Redis task
    push_branch_tasks[table] = PythonOperator(
        task_id=f'push_branch_redis_{table}',
        python_callable=push_branch_to_redis,
        op_kwargs={'table_name': table},
        provide_context=True,
        dag=dag
    )

    # Check changes task
    check_tasks[table] = SSHOperator(
        task_id=f'check_changes_{table}',
        ssh_hook=ssh_hook,
        command=spark_submit(
            f"batch/parking/SilverCheckMain_{table}.py"),
        dag=dag
    )

    # Push to Redis queue task
    push_queue_tasks[table] = PythonOperator(
        task_id=f'push_queue_{table}',
        python_callable=push_to_redis_queue,
        op_kwargs={'table_name': table},
        provide_context=True,
        dag=dag
    )

    # Sync to main task
    sync_tasks[table] = SSHOperator(
        task_id=f'sync_to_main_{table}',
        ssh_hook=ssh_hook,
        command=spark_submit(
            f"batch/parking/SilverSyncMain_{table}.py"),
        dag=dag
    )

# Commit changes task
commit_task = PythonOperator(
    task_id='commit_to_main',
    python_callable=commit_to_main,
    provide_context=True,
    dag=dag
)

# End task
end_dag = DummyOperator(task_id='end_dag', dag=dag)

# Set up dependencies
start_dag >> create_branch_task
commit_task >> end_dag
for table in tables:
    create_branch_task >> push_branch_tasks[table] >> check_tasks[table] >> \
        push_queue_tasks[table] >> sync_tasks[table] >> commit_task