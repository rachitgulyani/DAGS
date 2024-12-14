from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from datetime import datetime, timedelta

default_args={
    'owner':'rachit',
    'retries':5,
    'retry_interval':timedelta(minutes=2)
}

with DAG(
        default_args=default_args,
        dag_id='dag_with_posgres_connection',
        start_date=datetime(2024,12,1),
        catchup=False,
        schedule_interval='0 0 * * *'
 ) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
                create table if not exists dag_runs(
                dt date,
                dag_id varchar)
            """        
    )

    task2=PostgresOperator(
        task_id='Deleting_data_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            delete from  dag_runs where dt='{{ ds }}' and dag_id='{{dag.dag_id}}'
            """
    )

    task3=PostgresOperator(
        task_id='Inserting_data_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs(dt,dag_id) values('{{ ds }}', '{{dag.dag_id}}')
            """
    )

    task1>>task2>>task3