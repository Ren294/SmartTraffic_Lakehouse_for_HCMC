from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import json
from lakefs import Repository
from connection import get_redis_client, get_lakefs_client

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
    """Create a new branch for modified data based on current timestamp"""
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    branch_name = f"gasstation_{timestamp}_modified"

    # Create new branch from staging_gasstation
    repo.branch(branch_name).create(source_reference="staging_gasstation")

    context['task_instance'].xcom_push(
        key='modified_branch', value=branch_name)
    return branch_name


def push_to_redis_queue(table_name, **context):
    """Push table information to Redis queue for processing"""
    modified_branch = context['task_instance'].xcom_pull(
        task_ids='create_modified_branch', key='modified_branch')

    config = {
        'table_name': table_name,
        'modified_branch': modified_branch,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    redis_client = get_redis_client()
    redis_client.rpush(f"gasstation_modified_{table_name}", json.dumps(config))
    return config


def commit_changes(table_name, **context):
    """Commit changes to staging branch after successful update"""
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    staging_branch = repo.branch("staging_gasstation")

    commit_message = f"Updated {table_name} table with latest changes"
    commit = staging_branch.commit(message=commit_message)
    return {'status': 'success', 'commit_id': commit.get_commit().id}


# Create DAG
dag = DAG(
    'Silver_Staging_Gasstation_Validate_DAG',
    default_args=default_args,
    description='Sync PostgreSQL changes to Hudi tables',
    schedule_interval='@hourly',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

# Create SSH Hook
ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

# Start task
start_dag = DummyOperator(task_id='start_dag', dag=dag)

# Create modified branch task
create_branch_task = PythonOperator(
    task_id='create_modified_branch',
    python_callable=create_modified_branch,
    provide_context=True,
    dag=dag
)

# List of tables to process
tables = ['customer', 'employee', 'gasstation', 'inventorytransaction',
          'invoice', 'invoicedetail', 'product', 'storagetank']

# Create tasks for each table
check_tasks = {}
push_queue_tasks = {}
update_tasks = {}
commit_tasks = {}

for table in tables:
    # Check changes task
    check_tasks[table] = SSHOperator(
        task_id=f'check_changes_{table}',
        ssh_hook=ssh_hook,
        command=f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
        f"--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,"
        f"org.apache.hudi:hudi-spark3.2-bundle_2.12:0.15.0,"
        f"org.postgresql:postgresql:42.2.18 "
        f"/opt/spark-apps/batch/gasstation/SilverCheckChangesStaging_{
            table}.py",
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

    # Update Hudi table task
    update_tasks[table] = SSHOperator(
        task_id=f'update_hudi_{table}',
        ssh_hook=ssh_hook,
        command=f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
        f"--packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.15.0 "
        f"/opt/spark-apps/batch/gasstation/SilverUpdateStaging_{table}.py",
        dag=dag
    )

    # Commit changes task
    commit_tasks[table] = PythonOperator(
        task_id=f'commit_changes_{table}',
        python_callable=commit_changes,
        op_kwargs={'table_name': table},
        provide_context=True,
        dag=dag
    )

# End task
end_dag = DummyOperator(task_id='end_dag', dag=dag)

# Set up dependencies
start_dag >> create_branch_task

for table in tables:
    create_branch_task >> check_tasks[table] >> push_queue_tasks[table] >> \
        update_tasks[table] >> commit_tasks[table] >> end_dag
