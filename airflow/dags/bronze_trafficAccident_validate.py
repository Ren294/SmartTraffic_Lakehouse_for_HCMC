"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
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
    SCHEMA_DIR = os.path.join(os.path.dirname(
        os.path.dirname(__file__)), 'dags/schema')

    with open(os.path.join(SCHEMA_DIR, 'bronze_schema_traffic_accidents.json'), 'r') as schema_file:
        return json.load(schema_file)


def validate_traffic_accident_data(**context):
    lakefs_event = context['dag_run'].conf['lakeFS_event']
    branch_name = lakefs_event['branch_id']
    repository = context['dag_run'].conf['repository']

    branch_parts = branch_name.replace(
        "branch_trafficAccident_", "").split("_")
    location_info = branch_parts[0]
    accident_month = branch_parts[1]

    client = get_lakefs_client()
    repo = lakefs.repository(repository, client=client)
    branch = repo.branch(branch_name)

    data_path = f"traffic_accidents/{location_info}/{accident_month}"
    accident_obj = branch.object(path=data_path)

    if not accident_obj.exists():
        raise Exception(
            f"Traffic accident data not found at path: {data_path}")

    data = []
    with accident_obj.reader(mode='r') as reader:
        for line in reader:
            if line.strip():
                data.append(json.loads(line))

    schema = load_schema()

    road_name = location_info.split("-")[0]
    district = location_info.split("-")[1]

    for record in data:
        try:
            jsonschema.validate(instance=record, schema=schema)
        except jsonschema.exceptions.ValidationError as e:
            raise ValueError(f"Schema validation failed: {str(e)}")

        if record['road_name'].replace(" ", "") != road_name:
            raise ValueError(f"Road name mismatch: Expected \
              {road_name}, found {record['road_name']}")

        if record['district'].replace(" ", "") != district:
            raise ValueError(f"District mismatch: Expected \
              {district}, found {record['district']}")

        if record['accident_month'] != accident_month:
            raise ValueError(f"Month mismatch: Expected \
              {accident_month}, found {record['accident_month']}")

        if record['number_of_vehicles'] != len(record['vehicles_involved']):
            raise ValueError(f"Number of vehicles ({record['number_of_vehicles']}) doesn't match \
                vehicles_involved array length ({len(record['vehicles_involved'])})")

        accident_time = datetime.fromisoformat(record['accident_time'])
        recovery_time = datetime.fromisoformat(
            record['estimated_recovery_time'])
        if recovery_time <= accident_time:
            raise ValueError(
                "Estimated recovery time must be after accident time")

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

        repo = lakefs.repository(merge_request['repository'], client=client)
        source_branch = repo.branch(merge_request['source_branch'])
        target_branch = repo.branch(merge_request['target_branch'])

        try:
            changes = list(target_branch.diff(other_ref=source_branch))

            has_conflicts = False
            for change in changes:
                if change.type == 'modified':
                    source_obj = source_branch.object(change['path'])
                    target_obj = target_branch.object(change['path'])

                    if source_obj.exists() and target_obj.exists():
                        source_checksum = source_obj.stats()['checksum']
                        target_checksum = target_obj.stats()['checksum']
                        if source_checksum != target_checksum:
                            has_conflicts = True
                            break

            if not has_conflicts:
                merge_result = source_branch.merge_into(target_branch)
                return {
                    "merge_status": "success",
                    "branch": merge_request['source_branch'],
                    "commit_id": merge_request['commit_id'],
                    "merge_commit": merge_result
                }
            else:
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
    'Bronze_Traffic_accident_Validation_DAG',
    default_args=default_args,
    description='Validate traffic accident data and merge to main',
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
    task_id='validate_traffic_accident_data',
    python_callable=validate_traffic_accident_data,
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
