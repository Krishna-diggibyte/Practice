from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owner':'Krishna',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_with_catchup',
    description='Used Catchup and Backfill',
    start_date=datetime(2024,5,1),
    schedule_interval='@daily',
    catchup=True
) as dag:
    task1=BashOperator(
        task_id='task1',
        bash_command='echo I am testing catchup'
    )