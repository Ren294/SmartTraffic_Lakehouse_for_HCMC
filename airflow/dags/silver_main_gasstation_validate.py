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


def create_sync_branch(**context):
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    branch_name = f"gasstation_{timestamp}_sync"

    repo.branch(branch_name).create(source_reference="main")

    context['task_instance'].xcom_push(key='sync_branch', value=branch_name)
    return branch_name


def push_branch_to_redis(table_name, **context):
    sync_branch = context['task_instance'].xcom_pull(
        task_ids='create_sync_branch', key='sync_branch')

    redis_client = get_redis_client()
    redis_key = f"sync_branch_gasstation_{table_name}"

    redis_client.delete(redis_key)

    redis_client.rpush(redis_key, sync_branch)
    print(f"Pushed branch {sync_branch} to Redis key {redis_key}")

    return {'redis_key': redis_key, 'branch': sync_branch}


def push_to_redis_queue(table_name, **context):
    sync_branch = context['task_instance'].xcom_pull(
        task_ids='create_sync_branch', key='sync_branch')

    config = {
        'table_name': table_name,
        'sync_branch': sync_branch,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    redis_client = get_redis_client()
    redis_client.rpush(f"gasstation_sync_{table_name}", json.dumps(config))
    return config


def commit_to_main(**context):
    client = get_lakefs_client()
    repo = Repository("silver", client=client)
    main_branch = repo.branch("main")

    commit_message = f"Synced latest changes from staging to main"
    try:
        commit = main_branch.commit(message=commit_message)
        return {'status': 'success', 'commit_id': commit.get_commit().id}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}


dag = DAG(
    'Silver_Main_Gasstation_Sync_DAG',
    default_args=default_args,
    description='Sync data from staging to main branch',
    schedule_interval='@daily',
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

start_dag = DummyOperator(task_id='start_dag', dag=dag)

create_branch_task = PythonOperator(
    task_id='create_sync_branch',
    python_callable=create_sync_branch,
    provide_context=True,
    dag=dag
)

tables = ['customer', 'employee', 'gasstation', 'inventorytransaction',
          'invoice', 'invoicedetail', 'product', 'storagetank']

push_branch_tasks = {}
check_tasks = {}
push_queue_tasks = {}
sync_tasks = {}
check_file_checktask = {}
check_file_synctask = {}
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
        command=check_file_job(f"batch/gasstation/SilverCheckMain_{table}.py"),
        dag=dag
    )
    check_tasks[table] = SSHOperator(
        task_id=f'check_changes_{table}',
        ssh_hook=ssh_hook,
        command=spark_submit(
            f"batch/gasstation/SilverCheckMain_{table}.py"),
        dag=dag
    )

    push_queue_tasks[table] = PythonOperator(
        task_id=f'push_queue_{table}',
        python_callable=push_to_redis_queue,
        op_kwargs={'table_name': table},
        provide_context=True,
        dag=dag
    )
    check_file_synctask[table] = SSHOperator(
        task_id=f'check_sync{table}_job_file',
        ssh_hook=ssh_hook,
        command=check_file_job(f"batch/gasstation/SilverSyncMain_{table}.py"),
        dag=dag
    )
    sync_tasks[table] = SSHOperator(
        task_id=f'sync_to_main_{table}',
        ssh_hook=ssh_hook,
        command=spark_submit(
            f"batch/gasstation/SilverSyncMain_{table}.py"),
        dag=dag
    )

commit_task = PythonOperator(
    task_id='commit_to_main',
    python_callable=commit_to_main,
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
        check_file_checktask[table], check_file_synctask[table]]
    [check_file_checktask[table], check_file_synctask[table]] >> push_branch_tasks[table] >> check_tasks[table] >> \
        push_queue_tasks[table] >> sync_tasks[table] >> commit_task
