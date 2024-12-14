from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime,timedelta


default_args={
    'owner':'rachit',
    'retries':5,
    'retry_interval':timedelta(minutes=2)
}


with DAG(
    dag_id='dag_with_cron_expression',
    default_args=default_args,
    start_date=datetime(2024,12,1),
    schedule_interval='0  23 * * Tue'
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo Hello this is a dag with cron expression'
    )

    task1
 