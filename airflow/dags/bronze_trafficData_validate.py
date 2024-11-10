from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
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

    with open(os.path.join(SCHEMA_DIR, 'bronze_schema_traffic_data.json'), 'r') as schema_file:
        return json.load(schema_file)


def validate_traffic_data(**context):
    # Extract information from the lakeFS event in run config
    lakefs_event = context['dag_run'].conf['lakeFS_event']
    branch_name = lakefs_event['branch_id']
    repository = context['dag_run'].conf['repository']

    # Extract vehicle_id, month and date from branch name
    # Example: branch_trafficData_VH64581_2024-04_2024-04-07
    branch_parts = branch_name.replace("branch_trafficData_", "").split("_")
    vehicle_id = branch_parts[0]  # VH64581
    month = branch_parts[1]       # 2024-04
    date = branch_parts[2]        # 2024-04-07

    # Get the client and create repository object
    client = get_lakefs_client()
    repo = lakefs.repository(repository, client=client)
    branch = repo.branch(branch_name)

    # Read first record to get vehicle type and classification
    dummy_path = f"traffic_data/temp"
    dummy_obj = branch.object(path=dummy_path)

    if not dummy_obj.exists():
        raise Exception(
            "No data found to determine vehicle type and classification")

    with dummy_obj.reader(mode='r') as reader:
        first_record = json.loads(next(reader))
        vehicle_type = first_record['vehicle_type']
        vehicle_classification = first_record['vehicle_classification']

    # Construct the actual data path
    data_path = f"traffic_data/{vehicle_type}/\
      {vehicle_classification}/{vehicle_id}/{month}/{date}"
    traffic_obj = branch.object(path=data_path)

    if not traffic_obj.exists():
        raise Exception(f"Traffic data not found at path: {data_path}")

    # Read and parse data
    data = []
    with traffic_obj.reader(mode='r') as reader:
        for line in reader:
            if line.strip():  # Skip empty lines
                data.append(json.loads(line))

    # Load schema from file
    schema = load_schema()

    # Validation checks
    for record in data:
        # Check 1: Validate schema
        try:
            jsonschema.validate(instance=record, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            raise ValueError(f"Schema validation failed: {str(e)}")

        # Check 2: Validate vehicle_id consistency
        if record['vehicle_id'] != vehicle_id:
            raise ValueError(f"Vehicle ID mismatch: Expected \
              {vehicle_id}, found {record['vehicle_id']}")

        # Check 3: Validate month consistency
        if record['month'] != month:
            raise ValueError(f"Month mismatch: Expected \
              {month}, found {record['month']}")

        # Check 4: Validate date consistency
        if record['date'] != date:
            raise ValueError(f"Date mismatch: Expected \
              {date}, found {record['date']}")

        # Check 5: Validate vehicle type and classification consistency
        if record['vehicle_type'] != vehicle_type:
            raise ValueError(f"Vehicle type mismatch: Expected \
              {vehicle_type}, found {record['vehicle_type']}")

        if record['vehicle_classification'] != vehicle_classification:
            raise ValueError(f"Vehicle classification mismatch: Expected \
              {vehicle_classification}, found {record['vehicle_classification']}")

        # Check 6: Validate timestamp format and date consistency
        record_date = datetime.fromisoformat(
            record['timestamp']).strftime('%Y-%m-%d')
        if record_date != date:
            raise ValueError(f"Timestamp date mismatch: Expected \
              {date}, found {record_date}")

        # Check 7: Validate ETA is after timestamp
        timestamp = datetime.fromisoformat(record['timestamp'])
        eta = datetime.fromisoformat(
            record['estimated_time_of_arrival']['eta'])
        if eta <= timestamp:
            raise ValueError("ETA must be after timestamp")

    return {
        "validation": "success",
        "branch": branch_name,
        "commit_id": lakefs_event['commit_id'],
        "records_validated": len(data)
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
    'bronze_traffic_data_validation_dag',
    default_args=default_args,
    description='Validate traffic data and merge to main',
    schedule_interval=None,
    catchup=False
)
start_dag = DummyOperator(
    task_id='sstart_dag',
    dag=dag,
)

end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag,
)
validate_task = PythonOperator(
    task_id='validate_traffic_data',
    python_callable=validate_traffic_data,
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

start_dag >> validate_task >> queue_merge_task >> process_merge_task >> end_dag
