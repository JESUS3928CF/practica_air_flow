from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

#/ Importar esto
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag_owner = 'Jesus Cochero'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

#/ Ejemplo para ejecutar toda la query de un archivo

#! Por defecto airflow siempre buscara los archivos en la misma carpeta donde se esta ejecutando
#! Config para cambiar eso

with DAG(dag_id='',
        default_args=default_args,
        #* Supongamos que hay hay varios scripts
        template_searchpath=['/home/ubuntu/airflow/carpeta_random/scripts'], #/ especificar la ruta absoluta del archivo
        description='',
        start_date=datetime(),
        schedule_interval='',
        catchup=False,
        tags=['']
) :

    start = EmptyOperator(task_id='start')

    PostgresOperator_task = PostgresOperator(
    task_id='PostgresOperator_task',
    postgres_conn_id='postgres_default',
    sql='generate_train_and_test_tables.sql', #/ asi se llama el archivo al que le ejecutaremos todas las query's 
    database=None,
    runtime_parameters=None,
    )

    end = EmptyOperator(task_id='end')

    start >> PostgresOperator_task() >> end