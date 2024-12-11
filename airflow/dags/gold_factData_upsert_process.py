"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from lakefs import Repository
from airflow.operators.python import PythonOperator

from connection import get_redis_client, get_lakefs_client, spark_submit, check_file_job

DIMENSION_TABLES = [
    'fact_gasstationtransaction',
    'fact_parkingtransaction',
    'fact_trafficincident',
    'fact_vehiclemovement',
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Gold_Fact_Tables_WithUpsert_Load_DAG',
    default_args=default_args,
    description='Submit Spark jobs for dimension tables and commit to main',
    schedule_interval=None,
    catchup=False,
    concurrency=1,
    max_active_runs=1
)

ssh_hook = SSHHook(ssh_conn_id='spark_server', cmd_timeout=None)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)


def commit_changes(fact_name, **context):
    client = get_lakefs_client()
    repo = Repository("gold", client=client)
    staging_branch = repo.branch("main")
    commit_message = f"Load data to warehouse with {fact_name}"
    try:
        commit = staging_branch.commit(
            message=commit_message
        )
        return {
            'status': 'success',
            'commit_id': commit.get_commit().id,
            'branch': 'main'
        }
    except Exception as e:
        if fact_name == "all_fact":
            return {
                'status': 'success',
                'branch': 'main'
            }
        raise Exception(f"Failed to commit changes: {str(e)}")


commit_tasks = {}
submit_job_task = {}
check_file_task = {}

for table_name in DIMENSION_TABLES:
    check_file_task[table_name] = SSHOperator(
        task_id=f'check_{table_name}_job_file',
        ssh_hook=ssh_hook,
        command=check_file_job(f"gold/{table_name}_upsert.py"),
        dag=dag
    )

    submit_job_task[table_name] = SSHOperator(
        task_id=f'load_{table_name}_job',
        ssh_hook=ssh_hook,
        command=spark_submit(f"gold/{table_name}_upsert.py"),
        dag=dag
    )

    commit_tasks[table_name] = PythonOperator(
        task_id=f'commit_{table_name}_task',
        python_callable=commit_changes,
        op_kwargs={'fact_name': table_name},
        provide_context=True,
        dag=dag
    )

check_spark_connection = SSHOperator(
    task_id='check_connection_task',
    ssh_hook=ssh_hook,
    command='echo "Connection to Spark server successful"',
    dag=dag
)
commit_task = PythonOperator(
    task_id='commit_changes',
    python_callable=commit_changes,
    op_kwargs={'fact_name': 'all_fact'},
    provide_context=True,
    dag=dag
)
# start_dag >> check_spark_connection
# commit_task >> end_dag
# for table in DIMENSION_TABLES:
#     check_spark_connection >> check_file_task[table] >> submit_job_task[table] >> \
#         commit_tasks[table] >> commit_task
start_dag >> check_spark_connection
for i, table in enumerate(DIMENSION_TABLES):
    check_spark_connection >> check_file_task[table]
    if i > 0:
        commit_tasks[DIMENSION_TABLES[i-1]] >> submit_job_task[table]
    check_file_task[table] >> submit_job_task[table] >> commit_tasks[table]
for table in DIMENSION_TABLES:
    commit_tasks[table] >> commit_task
commit_task >> end_dag
