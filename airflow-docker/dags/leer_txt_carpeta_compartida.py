from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta 

dag_owner = 'Jesus Cochero'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
        }


def process_file():
    # Aquí puedes escribir la lógica para procesar el archivo desde la ruta compartida
    # Por ejemplo:
    with open("/opt/airflow/external_data/archivo.txt", 'r') as file:
        content = file.read()
        print(content)
        # Haz lo que necesites con el contenido del archivo

with DAG(dag_id='leer_archivo_compartido',
        default_args=default_args,
        description='',
        start_date=datetime.now(),
        schedule_interval=None,
        catchup=False,
        tags=['DeveloperJesus'],
        template_searchpath=['/opt/airflow/external_data']
) as dag:

    start = EmptyOperator(task_id='start')

    # Ejecuta la función de procesamiento de archivos
    process_file_task = PythonOperator(
        task_id='process_file_task',
        python_callable=process_file
    )

    end = EmptyOperator(task_id='end')

    start >> process_file_task >> end