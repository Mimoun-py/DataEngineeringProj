import sys
sys.path.insert(0, '/opt/airflow/pipeline')


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from main import run_pipeline

with DAG(
    dag_id="yellow_taxi_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule='10 15 5 5 *', 
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline
    )