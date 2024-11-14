from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from lakefs import Client, Repository

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def init_lakefs_client():
    return Client(
        endpoint="http://lakefs:8000/api/v1",
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    )


def validate_branch_data(repo: Repository, branch: str) -> bool:
    """
    Validate data in the branch before merging
    Add your validation logic here
    """
    # Example validation: Check if data exists
    try:
        objects = repo.objects.list(branch)
        return len(list(objects)) > 0
    except Exception:
        return False


def merge_branch_to_staging(**context):
    client = init_lakefs_client()
    repo = Repository("silver", client=client)

    date = context['dag_run'].conf.get('date')
    branch_name = context['dag_run'].conf.get('branch')

    if validate_branch_data(repo, branch_name):
        repo.merge(
            source_ref=branch_name,
            destination_branch="staging_weather",
            message=f"Merging hourly data from {branch_name}"
        )
    else:
        raise ValueError(f"Data validation failed for branch {branch_name}")


def merge_staging_to_main(**context):
    client = init_lakefs_client()
    repo = Repository("silver", client=client)

    if validate_branch_data(repo, "staging_weather"):
        repo.merge(
            source_ref="staging_weather",
            destination_branch="main",
            message="Merging daily data from staging_weather"
        )
    else:
        raise ValueError("Data validation failed for staging_weather branch")


# DAG for merging branch to staging
with DAG(
    'merge_weather_branch_to_staging',
    default_args=default_args,
    description='Merge weather branch to staging branch',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag1:
    merge_task = PythonOperator(
        task_id='merge_branch_to_staging',
        python_callable=merge_branch_to_staging,
        provide_context=True,
    )

# DAG for merging staging to main
with DAG(
    'merge_weather_staging_to_main',
    default_args=default_args,
    description='Merge weather staging to main branch',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag2:
    merge_task = PythonOperator(
        task_id='merge_staging_to_main',
        python_callable=merge_staging_to_main,
        provide_context=True,
    )
