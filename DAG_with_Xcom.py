from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime,timedelta


default_args={
        'owner':'rachit',
        'retries':5,
        'retry_delay':timedelta(minutes=5)
}


def greet(ti):
        first_name=ti.xcom_pull(task_ids='get_name',key='first_name')
        last_name=ti.xcom_pull(task_ids='get_name',key='last_name')
        age=ti.xcom_pull(task_ids='get_age_task',key='age')

        print(f"Hello World! My name is {first_name} {last_name}, ",
              f"and I am {age} years old")
        
def get_name(ti):
        #return 'Jerry'          To push only one value to xcom
        ti.xcom_push(key='first_name',value='Jerry')
        ti.xcom_push(key='last_name',value='Bhatia')

def get_age(ti):
        ti.xcom_push(key='age',value='25')


with DAG(        
        default_args=default_args,
        dag_id='DAG_with_PythonOperatorXcom',
        description='Second DAG with Python Operator',
        start_date=datetime(2024,12,1),
        schedule_interval='@daily'
) as dag:
         task1=PythonOperator(
                 task_id='greet',
                 python_callable=greet
                 # op_kwargs={'age':'24'}
         )

         task2=PythonOperator(
                 task_id='get_name',
                 python_callable=get_name
                 )
    
         task3=PythonOperator(
                task_id='get_age_task',
                python_callable=get_age
        )

[task2,task3]>>task1  