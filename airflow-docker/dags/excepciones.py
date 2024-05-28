from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

#- Necesitamos esto para el la excepción del tiempo 
import time
#/ Excepciones que pondremos en practica
from airflow.exceptions import AirflowSkipException,AirflowException,AirflowTaskTimeout
#from airflow.exceptions import AirflowSkipException #- Para omitir tareas
#from airflow.exceptions import AirflowException #- Nos permite controlar cuando una tarea falla
#from airflow.exceptions import AirflowTaskTimeout #- Para controlar el tiempo de espera



dag_owner = 'Jesús Cochero'

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=0.01)
}

params = {'manual': False}

#/
def saltar_tarea(**kwargs):
    params = kwargs.get('params', {})
    manual = params.get('manual', False)

    print('Antes de saltar')

    #- Se saltará esta tarea desde este punto del código y las que dependen de esta en este ejemplo end: saltar_tarea >> end
    if manual:
        raise AirflowSkipException("La tarea fue omitida")

#/  
def tarea_fallida(**kwargs):
    params = kwargs.get('params', {})
    manual = params.get('manual', False)

    print('Antes de re intentar o marcar como error depende el el retries')

    # Controlar error de fallo
    if manual:
        raise AirflowException("La tarea ha fallado")
    
#/   
def tiempo_tardado(**kwargs):
    params = kwargs.get('params', {})
    manual = params.get('manual', False)

    print('Antes de re intentar o marcar como error depende el retries')

    # Controlar error de fallo
    if manual:
        timeout = 10
        start_time = time.time()

        while True:
            elapsed_time = time.time() - start_time

            if elapsed_time > timeout:
                raise AirflowTaskTimeout("La tarea ha excedido el tiempo de espera especificado. se finalizara o se re intentar")
            time.sleep(1)


with DAG(
    dag_id='Excepciones',
    default_args=default_args,
    description='',
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=['Ejemplo'],
    params=params
) as dag:

    start = EmptyOperator(task_id='start')
    start02 = EmptyOperator(task_id='start02')
    start03 = EmptyOperator(task_id='start03')

    saltar_tarea_op = PythonOperator(
        task_id="saltar_tarea",
        python_callable=saltar_tarea,
        provide_context=True,
    )

    tarea_fallida_op = PythonOperator(
        task_id="tarea_fallida",
        python_callable=tarea_fallida,
        provide_context=True,
    )

    tiempo_tardado = PythonOperator(
        task_id="tiempo_tardado",
        python_callable=tarea_fallida,
        provide_context=True,
    )

    end = EmptyOperator(task_id='end')
    end02 = EmptyOperator(task_id='end02')
    end03 = EmptyOperator(task_id='end03')

    start >> saltar_tarea_op >> end
    start02 >> tarea_fallida_op >> end02
    start03 >> tiempo_tardado >> end03