from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta 
import pandas as pd

dag_owner = 'Jesus Cochero'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='leer_txt_local',
        default_args=default_args,
        description='',
        start_date=datetime.now(),
        schedule_interval=None,
        catchup=False,
        tags=['ETL'],
        template_searchpath=['/opt/airflow/external_data']
):

    start = EmptyOperator(task_id='start')

    def load_txt(**kwargs):
        #! Para leer un txt se usa pd.read_csv
        df_numbers = pd.read_sql('numeros.sql')
        print(df_numbers.head(2))
        print(df_numbers.columns)
    end = EmptyOperator(task_id='end')

start >> PythonOperator(task_id='load_txt', python_callable=load_txt) >> end