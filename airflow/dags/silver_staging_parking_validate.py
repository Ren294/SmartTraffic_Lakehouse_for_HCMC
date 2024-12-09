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
from connection import get_redis_client, get_lakefs_client, spark_submit, check_file_job

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
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    branch_name = f"parking_{timestamp}_modified"

    repo.branch(branch_name).create(source_reference="staging_parking")

    context['task_instance'].xcom_push(
        key='modified_branch', value=branch_name)
    return branch_name


def push_branch_to_redis(table_name, **context):
    modified_branch = context['task_instance'].xcom_pull(
        task_ids='create_modified_branch', key='modified_branch')

    redis_client = get_redis_client()
    redis_key = f"modified_branch_parking_{table_name}"

    redis_client.delete(redis_key)
    redis_client.rpush(redis_key, modified_branch)
    print(f"Pushed branch {modified_branch} to Redis key {redis_key}")

    return {'redis_key': redis_key, 'branch': modified_branch}


def push_to_redis_queue(table_name, **context):
    modified_branch = context['task_instance'].xcom_pull(
        task_ids='create_modified_branch', key='modified_branch')

    config = {
        'table_name': table_name,
        'modified_branch': modified_branch,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    redis_client = get_redis_client()
    redis_client.rpush(f"parking_modified_{table_name}", json.dumps(config))
    return config


def commit_changes(**context):
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    staging_branch = repo.branch("staging_parking")

    commit_message = f"Updated parking tables with latest changes"
    try:
        commit = staging_branch.commit(message=commit_message)
        return {'status': 'success', 'commit_id': commit.get_commit().id}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


dag = DAG(
    'Silver_Staging_Parking_Validate_DAG',
    default_args=default_args,
    description='Sync PostgreSQL changes to Hudi tables',
    schedule_interval='@hourly',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

start_dag = DummyOperator(task_id='start_dag', dag=dag)


create_branch_task = PythonOperator(
    task_id='create_modified_branch',
    python_callable=create_modified_branch,
    provide_context=True,
    dag=dag
)


tables = ['feedback', 'owner', 'parkinglot', 'parkingrecord',
          'payment', 'promotion', 'staff', 'vehicle']

push_branch_tasks = {}
check_tasks = {}
push_queue_tasks = {}
update_tasks = {}
commit_tasks = {}
check_file_checktask = {}
check_file_update = {}
start_dag_check_file = {}
for table in tables:
    push_branch_tasks[table] = PythonOperator(
        task_id=f'push_branch_redis_{table}',
        python_callable=push_branch_to_redis,
        op_kwargs={'table_name': table},
        provide_context=True,
        dag=dag
    )
    start_dag_check_file[table] = DummyOperator(
        task_id=f'start_check_{table}', dag=dag)

    check_file_checktask[table] = SSHOperator(
        task_id=f'check_check{table}_job_file',
        ssh_hook=ssh_hook,
        command=check_file_job(
            f"batch/parking/SilverCheckChangesStaging_{table}.py"),
        dag=dag
    )
    check_tasks[table] = SSHOperator(
        task_id=f'check_changes_{table}',
        ssh_hook=ssh_hook,
        command=spark_submit(
            f"batch/parking/SilverCheckChangesStaging_{table}.py"),
        dag=dag
    )

    push_queue_tasks[table] = PythonOperator(
        task_id=f'push_queue_{table}',
        python_callable=push_to_redis_queue,
        op_kwargs={'table_name': table},
        provide_context=True,
        dag=dag
    )
    check_file_update[table] = SSHOperator(
        task_id=f'check_update{table}_job_file',
        ssh_hook=ssh_hook,
        command=check_file_job(
            f"batch/parking/SilverUpdateStaging_{table}.py"),
        dag=dag
    )
    update_tasks[table] = SSHOperator(
        task_id=f'update_hudi_{table}',
        ssh_hook=ssh_hook,
        command=spark_submit(
            f"batch/parking/SilverUpdateStaging_{table}.py"),
        dag=dag
    )

    # commit_tasks[table] = PythonOperator(
    #     task_id=f'commit_changes_{table}',
    #     python_callable=commit_changes,
    #     op_kwargs={'table_name': table},
    #     provide_context=True,
    #     dag=dag
    # )


commit_task = PythonOperator(
    task_id='commit_changes',
    python_callable=commit_changes,
    provide_context=True,
    dag=dag
)
end_dag = DummyOperator(task_id='end_dag', dag=dag)
check_spark_connection = SSHOperator(
    task_id='check_connection_task',
    ssh_hook=ssh_hook,
    command='echo "Connection to Spark server successful"',
    dag=dag
)
start_dag >> check_spark_connection >> create_branch_task
commit_task >> end_dag
for table in tables:
    create_branch_task >> start_dag_check_file[table] >> [
        check_file_checktask[table], check_file_update[table]]
    [check_file_checktask[table], check_file_update[table]] >> push_branch_tasks[table] >> check_tasks[table] >> \
        push_queue_tasks[table] >> update_tasks[table] >> commit_task
