
#/ Importaciones necesarias
import pandas as pd


from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag_owner = 'Jesus Cochero'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(dag_id='read_csv',
         default_args=default_args,
         description='Leer un archivo de excel y robar todos sus datos',
         start_date=datetime.now(),
         schedule_interval=None,
         catchup=False,
         tags=['DeveloperJesus']
         ) as dag:

    start = DummyOperator(task_id='start')

    def load_wine(**kwargs):
        #! Cargar datos
        df_wine = pd.read_csv('/opt/airflow/dags/red_wine.csv')
        print(df_wine.head())
        print(df_wine.columns)

    def load_txt(**kwargs):
        #! Para leer un txt se usa pd.read_csv
        df_numbers = pd.read_csv('/opt/airflow/dags/numeber.txt')
        print(df_numbers.head(2))
        print(df_numbers.columns)

    end = DummyOperator(task_id='end')

    start >> PythonOperator(task_id='load_wine', python_callable=load_wine) >> end
    start >> PythonOperator(task_id='load_txt', python_callable=load_txt) >> end