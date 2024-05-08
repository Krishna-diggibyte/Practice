from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner':'krishna',
    'retries':5,
    'retry-delay':timedelta(minutes=5)
}
def intro():
    print("My self Krishna")

with DAG(
    default_args=default_args,
    dag_id='dag_with_python_operator',
    description="my first dag using python operator",
    start_date=datetime(2024,5,5),
    schedule_interval='@daily'

)as dag:
    task1=PythonOperator(
        task_id='first_python',
        python_callable=intro
    )
    
task1