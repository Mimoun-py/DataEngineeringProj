import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from datetime import datetime


INPUT_FILE_PATH = '/opt/airflow/input/ecommerce_orders.csv'


def file_exists():
    import os
    return os.path.exists('/opt/airflow/input/ecommerce_orders.csv')


def run_pipeline_task():
    sys.path.insert(0, '/opt/airflow/pipeline_rt')
    from main import run_pipeline
    run_pipeline(file_path='/opt/airflow/input/ecommerce_orders.csv')


with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    wait_for_file = PythonSensor(
        task_id="wait_for_input_file",
        python_callable=file_exists,
        poke_interval=10,
        timeout=600,
        mode='poke',
    )

    run_pipeline = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline_task,
    )

    wait_for_file >> run_pipeline
