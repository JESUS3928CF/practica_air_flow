from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta 

dag_owner = 'Jesus Cochero'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

def titulo():
        return 'El uso de Airflow en la universidad de Springfield'
    
def introduccion():
        return "El otro día mi hija me dijo que Airflow no se utilizaba en la universidad de Springfield, y yo le dije: qué no Lisa? qué no?"

def relleno():
        for _ in range(100):
            print("Púdrete Flanders")

with DAG(dag_id='escrito_de_Homero',
        default_args=default_args,
        start_date=datetime(2024, 5, 19),
        schedule_interval='* 19 * * *'
) as dag: 

    start = DummyOperator(task_id='start')

    titulo = PythonOperator(
        task_id="titulo",
        python_callable=titulo,
    )

    introduccion = PythonOperator(
        task_id="introducción",
        python_callable=introduccion,
    )

    relleno = PythonOperator(
        task_id="relleno",
        python_callable=relleno,
    )

    end = DummyOperator(task_id='end')

start >> titulo >> introduccion >> relleno >> end