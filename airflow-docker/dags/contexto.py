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

<<<<<<< HEAD
=======
#! NO TODOS LOS OPERADORES PUEDEN RECIBIR EL CONTEXT Para eso hay otra forma 
#/ Aca definimos los parámetros
>>>>>>> e62782811e1ff40431daee1ae433134301970c29
params = {
    'Manual': False
}

<<<<<<< HEAD

def first_task (**kwargs):
    kwargs['ti'].xcom_push(key='color', value = 'Amarillo')

#/step 2. y en el método lo revivimos como parámetro
def execute_task (ds, color): #- también la recibimos como parámetro en el método 
    print('Fecha de ejecución ', ds)
    print('Color de la tarea anterior' , color)


=======
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
>>>>>>> e62782811e1ff40431daee1ae433134301970c29

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
<<<<<<< HEAD
        #provide_context=True
        #/step 1) le damos el nombre y el valor por aca
        op_kwargs={'ds': '{{ ds }}', #- esta es una variable por defecto del contexto
                   'color': "{{ti.xcom_pull(task_ids='first_task', key='color')}}" }, #- asi extraemos una variable de otro operador, con la key y nombre del operador
=======
        provide_context=True #- Para que python pueda recibir el contexto
    )

    context_task = PythonOperator(
        task_id="context_task",
        python_callable=execute_task,
        provide_context=True #- Para que python pueda recibir el contexto
>>>>>>> e62782811e1ff40431daee1ae433134301970c29
    )

    end = EmptyOperator(task_id='end')

<<<<<<< HEAD
    start >> python_task >> end
=======
start >> python_task >> context_task >> end
>>>>>>> e62782811e1ff40431daee1ae433134301970c29
