from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='',
        default_args=default_args,
        description='',
        start_date=datetime(),
        schedule_interval='',
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''

    end = EmptyOperator(task_id='end')

    start >> task_1() >> end