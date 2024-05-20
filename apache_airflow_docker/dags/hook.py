
#/ importar pandas 
import pandas as pd
import logging

from airflow import DAG
from airflow.utils import dates
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator 

#/ Lo primero importarlo
from airflow.hooks.postgres_hook import PostgresHook


default_args = {'start_date': dates.days_ago(1)}

#/ 2) Crear una instancia de se hook 
def obtener_pandas():
    conn_hook = PostgresHook(
        postgres_conn_id='postgres_produccion'
    )
    #/ Usar una de las distintas funciones que tiene
    #- es función lo que hace es trasformar la info en un data frame  
    df = conn_hook.get_pandas_df("SELECT * FROM TABLE")
    logging.info("Datos obtenidos de la query")
    df.to_csv('s3://bucket/key.csv', index=False) #- ruta donde se almacena
    logging.info("Datos guardados en S3")

with DAG(
    'dag_hook',
    default_args=default_args,
    description='A simple tutorial DAG'
) as dag:
    
    start = DummyOperator(
        task_id="inicio"
    )

    obtener_pandas_operator = PythonOperator(task_id='Obtener_pandas_operator', python_callable=obtener_pandas)

    end = DummyOperator(
        task_id="fin"
    )