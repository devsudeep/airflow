from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def _start():
    print('Starting the DAG')

def _end():
    print('ending the DAG bye bye')
with DAG('my_dag', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    # Define your tasks here
    start = PythonOperator(task_id='start', python_callable=_start)

    end = PythonOperator(task_id='end', python_callable=_end)

    start >> end # defining the order of execution