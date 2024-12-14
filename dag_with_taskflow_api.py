from airflow.decorators import dag, task
from datetime import  datetime,timedelta



default_args={
    'owner':'rachit',
    'retries':5,
    'retrt_delay':timedelta(minutes=2)
}

'''

#with DAG(
#) as dag:

@dag(dag_id='dag_with_airflow_api', 
     default_args=default_args,
     start_date=datetime(2024,12,2),
     schedule_interval='@daily'  
     
     )
def hello_world_etl():
    
    @task()
    def get_name():
        return 'Jerry'

    @task()
    def get_age():
        return '19'
    
    @task()
    def greet(name,age):
        print(f'Hello! I am {name} and I am {age} yrs old.')


    name=get_name()
    age=get_age()
    greet(name=name,age=age)

greet_dag=hello_world_etl()


'''



@dag(dag_id='dag_with_airflow_api_v02', 
     default_args=default_args,
     start_date=datetime(2024,12,2),
     schedule_interval='@daily'  
     
     )

def hello_world_etl():
    
    @task(multiple_outputs=True)
    def get_name():
        return {'first_name':'Jerry',
                'last_name':'Duncan'}
    

    @task()
    def get_age():
        return '19'
    
    @task()
    def greet(first_name,last_name,age):
        print(f'Hello! I am {first_name} {last_name} and I am {age} yrs old.')


    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'], 
          last_name=name_dict['last_name'],
          age=age)

greet_dag=hello_world_etl()