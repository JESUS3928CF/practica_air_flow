from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('ejecutar_script_py',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    prueba_bash = BashOperator(
        task_id='prueba_bash',
        bash_command='xvfb-run -a python3 /opt/airflow/dags/generador_de_facturas.py',
    )
