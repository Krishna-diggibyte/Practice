from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
default_args={
    'owner':'krishna',
    'retries':5,
    'retry-delay':timedelta(minutes=5)
}
def get_name(ti):
    ti.xcom_push(key='name',value='Krishna')
    ti.xcom_push(key='city',value='Delhi')
def greet(ti):
    name=ti.xcom_pull(task_ids='second_task',key='name')
    city=ti.xcom_pull(task_ids='second_task',key='city')
    print(f"Hi All, My self {name}, and i am from {city}.")

with DAG(
    default_args=default_args,
    dag_id='python_with_xcom',
    description='Python operator with parameters',
    start_date=datetime(2024,5,6),
    schedule_interval='@daily'
) as dag:
    task1=PythonOperator(
        task_id='task_first',
        python_callable=greet
    )
    task2=PythonOperator(
        task_id='second_task',
        python_callable=get_name
    )
task2>>task1