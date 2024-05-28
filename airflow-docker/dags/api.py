from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

#/ Principales importaciones
import requests
import json

dag_owner = 'Jesus'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

#/ Ejemplo para obtener datos de un json
def get_api_data():
    url = "https://csrng.net/csrng/csrng.php?min=0&max=150"
    response = requests.get(url)
    data = response.json()
    return data

#/ Imprime los resultados
def print_json(**kwargs):
    ti = kwargs['ti']
    api_data = ti.xcom_pull(task_ids='get_api_data')
    print(json.dumps(api_data, indent=2))

with DAG(dag_id='leer_api',
        default_args=default_args,
        description='',
        start_date=datetime.now(),
        schedule_interval=timedelta(days=1),  # Ejecutar diariamente
        catchup=False,
        tags=['ETL']
):

    start = EmptyOperator(task_id='start')

    get_api_task = PythonOperator(
        task_id='get_api_data',
        python_callable=get_api_data,
        provide_context=True
    )

    print_json_task = PythonOperator(
        task_id='print_json',
        python_callable=print_json,
        provide_context=True
    )
    
    end = EmptyOperator(task_id='end')

    start >> get_api_task >> print_json_task >> end
