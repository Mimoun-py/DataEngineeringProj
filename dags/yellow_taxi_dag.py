import sys
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def run_pipeline_task():
    sys.path.insert(0, '/opt/airflow/pipeline')
    from main import run_pipeline
    run_pipeline()


with DAG(
    dag_id="yellow_taxi_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz='Europe/Brussels'),
    schedule='54 14 5 5 *',
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline_task,
    )