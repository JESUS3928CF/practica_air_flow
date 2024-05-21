from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta 

dag_owner = 'Jesus Cochero'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
        }

with DAG(dag_id='ejecutar_script_python',
        default_args=default_args,
        description='Ejecutar un script de python externo',
        start_date=datetime.now(),
        schedule_interval=None,
        catchup=False,
        tags=['DeveloperJesus']
):

    start = EmptyOperator(task_id='start')

    def ejecutar_script():
        # Llama a la función importada desde tu script externo
        from scripts.dag import print_hello
        print_hello()

    # Define el operador PythonOperator y pasa la función que ejecutará tu script de Python
    ejecutar_script_op = PythonOperator(
        task_id='ejecutar_script',
        python_callable=ejecutar_script
    )

    end = EmptyOperator(task_id='end')

    # Define el flujo del DAG
    start >> ejecutar_script_op >> end