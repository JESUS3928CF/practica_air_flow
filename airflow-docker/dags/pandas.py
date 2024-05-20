from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta 

#/ Importar
import pandas as pd

dag_owner = 'Jesus Cochero'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='pandas_ejemplo',
    default_args=default_args,
    description='Example DAG using Pandas in Airflow',
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=['ETL']
) as dag:

    start = DummyOperator(task_id='start')

    #! Empezamos a usar PANDAS
    @task
    def process_json():
        #- Supongamos que este es tu objeto JSON
        json_data = '{"name": ["John", "Alice", "Bob", "Alice", "Jesús"], "age": [30, 25, 35, 25,20]}'
        
        #/ Convertir el JSON a un DataFrame
        df = pd.read_json(json_data)
        
        #/ Eliminar duplicados basados en todas las columnas
        df = df.drop_duplicates()
        
        return df

    @task
    def print_dataframe(df):
        #/ Imprimir las primeras tres filas del DataFrame
        print(df.head(4))

    end = DummyOperator(task_id='end')

    start >> process_json() >> print_dataframe(process_json()) >> end
