from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args={
    'owner':'rachit',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def print_func():
    print('Hello Python here!!!!')
    return 0

with DAG(
        dag_id='our_first_dag',
        default_args=default_args,
        description='Our first dag in creation',
        start_date=datetime(2024,11,30),
        schedule_interval='@daily'
) as dag:
    task1=BashOperator(
        task_id='first_task',
        bash_command="echo hello world, This is our first task!"
    )

    task2=BashOperator(
        task_id='second_task',
        bash_command="echo Hi, I am the second task and I will be running after first task!"
        )
    
    task3=BashOperator(
        task_id='third_task',
        bash_command="echo Hi, I am the third task and I will be running after first task with second task!"
    )

    task4=PythonOperator(
        task_id='fourth_task',
        python_callable=print_func)

    # Task dependency method 1
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # Task dependency method 2
    # task1 >> task2
    # task1 >> task3
    
    task1>>[task2,task3]>>task4