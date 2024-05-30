from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator 
from datetime import datetime, timedelta 

#/ Importar esto
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator
import ast

dag_owner = 'Jesus'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1),
}

#/ Tarea de bifurcación
def bifurcación(**context):
    #/ Recuperar el valor del xcom
    xcom_value = context['ti'].xcom_pull(task_ids='api_call')
    print(xcom_value)

    response_list = ast.literal_eval(xcom_value) #- esto lo que hace es pasar de string a lo que se le parezca   

    number = response_list[0]['random'] #- esta es la naturaleza de el objeto

    #/ Si el valor es mayor que 5 se ejecuta la tarea 1, si no se ejecuta la tarea 2
    if number > 5:
        return 'task_mayor_que_5'
    else:
        return 'task_menor_que_5'

with DAG(
    dag_id='llamada_api_bifurcacion',
    default_args=default_args,
    description='DAG para llamar a una API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    #/ llamada a la api y guardando el valor en el xcom
    api_call = SimpleHttpOperator(
        task_id='api_call',
        http_conn_id='random_number',
        endpoint='/csrng/csrng.php',
        method='GET',
        data={'min': 0, 'max': 10}, #- esto es propio de la api son los parámetros
        xcom_push=True, #- recuerda que el contexto se maneja asi en la version 1
    )

    #/ Tarea de bifurcación 
    bifurcacion_task = BranchPythonOperator(task_id='bifurcacion_task',
                                            python_callable=bifurcación,
                                            provide_context=True
                                            )
    
    #/ Definiendo nuestras tareas que se ejecutara gracias a la bifurcacion_task
    task_mayor_que_5 = DummyOperator(task_id='task_mayor_que_5')

    task_menor_que_5 = DummyOperator(task_id='task_menor_que_5')

    end = DummyOperator(task_id='end')

    #/ Al momento de relacionar las tareas de bifurcacion meterlos en un array como paralelas
    start >> api_call >> bifurcacion_task >> [task_mayor_que_5, task_menor_que_5] >> end
