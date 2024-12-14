from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args={
    'owner':'rachit',
    'retries':5,
    'retry_interval':timedelta(minutes=2)
}


with DAG(
        dag_id='dag_with_catchup_backfill_v02',
        default_args=default_args,
        start_date=datetime(2024,12,1),
        schedule_interval='@daily',
        catchup=False        
) as dag:
        task1=BashOperator(
        task_id='task1',
        bash_command='echo This is a simple bash command!',
        )


task1