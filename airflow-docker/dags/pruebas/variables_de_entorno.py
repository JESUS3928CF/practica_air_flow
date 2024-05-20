
from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator 
#/ modulo necesario
from airflow.models import Variable
 
#/ Declaramos las variables y les  asignamos el valor
PASS = Variable.get('pass')
ENDPOINT = Variable.get('endpoint')

TAGS = ['DeveloperJesus'] 
DAG_ID = "VARIABLES" 
DAG_DESCRIPTION = """ variables de entorno """
DAG_SCHEDULE = "40 20 * * *" 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 13), 
    'email_on_failure': False,
    'email_on_retry': False,

    #* estableemos los reintentos, esto se puede hacer por aca parece
    'retries': 4,
    'retry_delay': timedelta(minutes=5), 
}

#* estableemos los reintentos
retries = 4
retry_delay = timedelta(minutes=5) 


#/ 3) USANDO ESA VARIABLES
def execute_task ():
    print("pass: ", PASS)
    print("End Point: ", ENDPOINT)


dag = DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    catchup=False, 
    schedule_interval=DAG_SCHEDULE,
    max_active_runs=1, 
    dagrun_timeout=200000,
    default_args=default_args,
    tags=TAGS
)


#/ 4 crear las tareas que irán asociadas a ese dag
with dag as dag:
    #* esta tarea es un operador dummy que no hace nada solo es una bandera que indica cuando el proceso inicio
    start_task = EmptyOperator(
        task_id='start_task',
    )

    #* marca la finalización
    end_task = EmptyOperator(
        task_id='end_task',
    )

    first_task = PythonOperator(
        task_id='first_task',
        python_callable=execute_task,
        retries=retries,
        retry_delay=retry_delay,
    )


start_task >> first_task >> end_task 