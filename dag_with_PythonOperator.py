from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime,timedelta


default_args={
        'owner':'rachit',
        'retries':5,
        'retry_delay':timedelta(minutes=5)
}


def greet(name,age):
        print(f"Hello World! My name is {name}, ",
              f"and I am {age} years old")
        
def get_name():
        return 'Jerry'


with DAG(        
        default_args=default_args,
        dag_id='PythonOperatorDAG',
        description='Second DAG with Python Operator',
        start_date=datetime(2024,12,1),
        schedule_interval='@daily'
) as dag:
         task1=PythonOperator(
                 task_id='greet',
                 python_callable=greet,
                 op_kwargs={'name':'Rachit','age':'24'}
         )

         task2=PythonOperator(
                 task_id='get_name',
                 python_callable=get_name
                 )
    

task1>>task2 