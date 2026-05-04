import sys
import os
import shutil
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.api.common.trigger_dag import trigger_dag
from pendulum import timezone
from datetime import datetime


INPUT_FILE_PATH = '/opt/airflow/input/ecommerce_orders.csv'
USED_DIR        = '/opt/airflow/input/used'


def file_exists():
    return os.path.exists(INPUT_FILE_PATH)


def run_pipeline_task():
    sys.path.insert(0, '/opt/airflow/pipeline_rt')
    from main import run_pipeline
    run_pipeline(file_path=INPUT_FILE_PATH)


def move_to_used():
    os.makedirs(USED_DIR, exist_ok=True)
    filename = os.path.basename(INPUT_FILE_PATH)
    dest     = os.path.join(USED_DIR, filename)
    shutil.move(INPUT_FILE_PATH, dest)
    print(f"Moved {filename} to {USED_DIR}")


def retrigger():
    trigger_dag(dag_id="ecommerce_pipeline")


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone("Europe/Brussels")),
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_input_file",
        python_callable=file_exists,
        poke_interval=10,
        timeout=604800,
        mode='reschedule',
    )

    run_pipeline_task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline_task,
    )

    move_file = PythonOperator(
        task_id="move_to_used",
        python_callable=move_to_used,
    )

    retrigger_task = PythonOperator(
        task_id="retrigger_dag",
        python_callable=retrigger,
    )

    wait_for_file >> run_pipeline_task >> move_file >> retrigger_task