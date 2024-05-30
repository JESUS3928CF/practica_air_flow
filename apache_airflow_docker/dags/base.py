from airflow import DAG
#from airflow.decorators import task  # Código comentado
#from airflow.operators.empty import EmptyOperator  # Código comentado
from airflow.operators.dummy_operator import DummyOperator 
from datetime import datetime, timedelta 

dag_owner = 'Jesus'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=0.01)
}

with DAG(
    dag_id='basico_cambios',
    default_args=default_args,
    description='',
    start_date=datetime(2024, 5, 1),  # Define una fecha de inicio adecuada
    schedule_interval=None,
    catchup=False,
    #tags=['Dev']
):

    start = DummyOperator(task_id='start')

    end = DummyOperator(task_id='end')

    start >> end
