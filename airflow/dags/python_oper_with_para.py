from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner':'krishna',
    'retries':5,
    'retry-delay':timedelta(minutes=5)
}

def greet(name,age):
    print(f"Hi All, My self {name}, and i am {age} year old")

with DAG(
    default_args=default_args,
    dag_id='python_with_para',
    description='Python operator with parameters',
    start_date=datetime(2024,5,6),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id='task_first',
        python_callable=greet,
        op_kwargs={'name':'Krishna','age':23}
    )