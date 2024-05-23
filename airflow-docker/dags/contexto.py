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

#! NO TODOS LOS OPERADORES PUEDEN RECIBIR EL CONTEXT Para eso hay otra forma 
#/ Aca definimos los parámetros
params = {
    'Manual': False
}

#/ el **kwargs es para poder acceder al contexto del dag
def execute_task (**kwargs):
    print(kwargs) #- para ver el contexto impreso

    #! USAR EL CONTEXTO
    params = kwargs.get('params', {})
    manual = params.get('Manual', False)#- si no lo encuentra lo deja como false

    if manual: 
        #/ asi añadimos un valor al contexto
        kwargs['ti'].xcom_push(key='color', value='Amarillo')
    else: 
        kwargs['ti'].xcom_push(key='color', value='Rojo')

def context_task(**kwargs):
    ti = kwargs['ti']

    #- Asi traemos ese valor de la tarea anterior
    color =  ti.xcom_pull(task_ids='python_task', key='color')
    print(color)

with DAG(dag_id='contexto_dag',
        default_args=default_args,
        description='usando el contexto de las tareas',
        start_date=datetime.now(),
        schedule_interval=None,
        catchup=False,
        tags=['DeveloperJesus'],
        params=params #- recibir los parámetros para este dag
):

    start = EmptyOperator(task_id='start')

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=execute_task,
        provide_context=True #- Para que python pueda recibir el contexto
    )

    context_task = PythonOperator(
        task_id="context_task",
        python_callable=execute_task,
        provide_context=True #- Para que python pueda recibir el contexto
    )

    end = EmptyOperator(task_id='end')

start >> python_task >> context_task >> end