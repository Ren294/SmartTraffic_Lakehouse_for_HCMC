from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import jsonschema
import os
from connection import get_redis_client, get_lakefs_client
import lakefs


def load_schema():
    """Load JSON schema from file"""
    SCHEMA_DIR = os.path.join(os.path.dirname(
        os.path.dirname(__file__)), 'dags/schema')

    with open(os.path.join(SCHEMA_DIR, 'bronze_schema_weather.json'), 'r') as schema_file:
        return json.load(schema_file)


def validate_weather_data(**context):
    # Extract information from the lakeFS event in run config
    lakefs_event = context['dag_run'].conf['lakeFS_event']
    branch_name = lakefs_event['branch_id']  # Changed from branch_name
    repository = context['dag_run'].conf['repository']

    # Extract date from branch name (branch_weather_2024-03-20 -> 2024-03-20)
    date_str = branch_name.replace("branch_weather_", "")

    # Get the client and create repository object
    client = get_lakefs_client()
    repo = lakefs.repository(repository, client=client)
    branch = repo.branch(branch_name)

    # Read weather data
    weather_path = f"weather/{date_str}-HCMC"
    weather_obj = branch.object(path=weather_path)

    if not weather_obj.exists():
        raise Exception(f"Weather data not found at path: {weather_path}")

    # Read and parse data
    data = []
    with weather_obj.reader(mode='r') as reader:
        for line in reader:
            data.append(json.loads(line))

    # Load schema from file
    schema = load_schema()

    # Check 1: Validate number of records
    if len(data) != 24:
        raise ValueError(f"Expected 24 hourly records, but found {len(data)}")

    # Check 2: Validate date consistency
    for record in data:
        if record['date'] != date_str:
            raise ValueError(f"Date mismatch: Expected \
              {date_str}, found {record['date']}")

    # Check 3: Validate schema for each record
    for record in data:
        try:
            jsonschema.validate(instance=record, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            raise ValueError(f"Schema validation failed: {str(e)}")

    # Check 4: Validate hours sequence
    hours = sorted([int(record['datetime'].split(':')[0]) for record in data])
    expected_hours = list(range(24))
    if hours != expected_hours:
        raise ValueError("Missing or duplicate hours in data")

    return {
        "validation": "success",
        "branch": branch_name,
        "commit_id": lakefs_event['commit_id']
    }


def queue_merge(**context):
    redis_client = get_redis_client()
    lakefs_event = context['dag_run'].conf['lakeFS_event']
    branch_name = lakefs_event['branch_id']

    merge_request = {
        "repository": context['dag_run'].conf['repository'],
        "source_branch": branch_name,
        "target_branch": "main",
        "commit_id": lakefs_event['commit_id'],
        "timestamp": datetime.now().isoformat()
    }

    redis_client.rpush("merge_queue", json.dumps(merge_request))
    return {"queued_branch": branch_name}


def process_merge_queue(**context):
    redis_client = get_redis_client()
    client = get_lakefs_client()

    merge_request_str = redis_client.lpop("merge_queue")
    if merge_request_str:
        merge_request = json.loads(merge_request_str)

        # Create repository object using lakefs package
        repo = lakefs.repository(merge_request['repository'], client=client)
        source_branch = repo.branch(merge_request['source_branch'])
        target_branch = repo.branch(merge_request['target_branch'])

        try:
            # Get changes between source and target branches
            changes = list(target_branch.diff(other_ref=source_branch))

            # Analyze the changes to detect real conflicts
            has_conflicts = False
            for change in changes:
                if change.type == 'modified':
                    # Check if the same file was modified in both branches
                    source_obj = source_branch.object(change['path'])
                    target_obj = target_branch.object(change['path'])

                    if source_obj.exists() and target_obj.exists():
                        # If file exists in both branches, check if they're different
                        source_checksum = source_obj.stats()['checksum']
                        target_checksum = target_obj.stats()['checksum']
                        if source_checksum != target_checksum:
                            has_conflicts = True
                            break

            if not has_conflicts:
                # If no real conflicts found, proceed with merge
                merge_result = source_branch.merge_into(target_branch)
                return {
                    "merge_status": "success",
                    "branch": merge_request['source_branch'],
                    "commit_id": merge_request['commit_id'],
                    "merge_commit": merge_result
                }
            else:
                # If conflicts found, requeue the merge request
                redis_client.rpush("merge_queue", merge_request_str)
                return {
                    "merge_status": "requeued",
                    "branch": merge_request['source_branch'],
                    "reason": "conflicts detected",
                    "conflict_details": changes
                }

        except Exception as e:
            return {
                "merge_status": "error",
                "branch": merge_request['source_branch'],
                "error": str(e)
            }


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
    'bronze_weather_validation_dag',
    default_args=default_args,
    description='Validate weather data and merge to main',
    schedule_interval=None,
    catchup=False
)

validate_task = PythonOperator(
    task_id='validate_weather_data',
    python_callable=validate_weather_data,
    provide_context=True,
    dag=dag
)

queue_merge_task = PythonOperator(
    task_id='queue_merge',
    python_callable=queue_merge,
    provide_context=True,
    dag=dag
)

process_merge_task = PythonOperator(
    task_id='process_merge',
    python_callable=process_merge_queue,
    provide_context=True,
    dag=dag
)

validate_task >> queue_merge_task >> process_merge_task
