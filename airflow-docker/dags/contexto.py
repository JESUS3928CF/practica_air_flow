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

params = {
    'Manual': False
}


def first_task (**kwargs):
    kwargs['ti'].xcom_push(key='color', value = 'Amarillo')

#/step 2. y en el método lo revivimos como parámetro
def execute_task (ds, color): #- también la recibimos como parámetro en el método 
    print('Fecha de ejecución ', ds)
    print('Color de la tarea anterior' , color)



with DAG(dag_id='contexto_dag',
        default_args=default_args,
        description='usando el contexto de las tareas',
        start_date=datetime.now(),
        schedule_interval=None,
        catchup=False,
        tags=['DeveloperJesus']
):

    start = EmptyOperator(task_id='start')

    first_task = PythonOperator(
        task_id="first_task",
        python_callable=first_task,
        provide_context=True
    )

    #-info Ahora como existen operadores que no reciben la funcionalidad de provide context pero si resiven parametros
    #-info podemos utilizar el parametro op_kwargs para pasar el contexto
    python_task = PythonOperator(
        task_id="python_task",
        python_callable=execute_task,
        #provide_context=True
        #/step 1) le damos el nombre y el valor por aca
        op_kwargs={'ds': '{{ ds }}', #- esta es una variable por defecto del contexto
                   'color': "{{ti.xcom_pull(task_ids='first_task', key='color')}}" }, #- asi extraemos una variable de otro operador, con la key y nombre del operador
    )

    end = EmptyOperator(task_id='end')

    start >> python_task >> end