from datetime import timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

from datetime import datetime


def first_function(**context):
    print("Hello world this works ")


def on_failure_callback(context):
    print("Fail works  !  ")


with DAG(dag_id="first_dag_github",
         schedule_interval="@once",
         default_args={
             "owner": "airflow",
             "start_date": datetime(2020, 11, 1),
             "retries": 1,
             "retry_delay": timedelta(minutes=1),
             'on_failure_callback': on_failure_callback,
             'email': ['ibhandarikrishna@gmail.com'],
             'email_on_failure': False,
             'email_on_retry': False,
         },
         catchup=False) as dag:

    first_function = PythonOperator(
        task_id="first_function",
        python_callable=first_function,
    )

    email = EmailOperator(
        task_id='send_email',
        to='ibhandarikrishna@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test Airflow </h3> """,
    )


first_function >> email






