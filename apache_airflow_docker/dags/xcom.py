from datetime import datetime

from airflow.models import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 

argumentos_por_defecto = {
    'owner': 'Jesus Cochero',
    'start_date' : datetime(2024, 5, 19, 19, 29, 4),
}

#! Hay 3 formas de hacer un push
#- 1) Dentro de un python operator


#- 2
def hello_world_loop(**context):
    for palabra in ['Hello', 'World']:
        print(palabra)
    
    task_instance = context['task_instance']
    task_instance.xcom_push(key='clave', value='valor de prueba')
    return 'hola'

def imprimir_valor_xcom(**context):

    #/ Asi traemos un valor del xcom
    ti = context['task_instance']
    valor_cogido = ti.xcom_pull(task_ids = 'prueba_python')

    print(valor_cogido)

with DAG(dag_id='xcom',
         default_args=argumentos_por_defecto,
         schedule_interval='@once'
         ) as dag: 
    start = DummyOperator(task_id='inicio')

    prueba_python = PythonOperator(task_id='prueba_python', 
                                   python_callable=hello_world_loop,
                                   do_xcom_push=True, #- Esta es la forma 2
                                   provide_context=True
                                   )
    
    #- definición del operador
    prueba_pull = PythonOperator(task_id='prueba_pull', 
                                python_callable=imprimir_valor_xcom,
                                do_xcom_push=True, #- Esta es la forma 2
                                provide_context=True
                                )
    

    #! La forma de ejecutar archivos de python no es desde el python operator si no desde el bash operator
    prueba_bash = BashOperator(task_id='prueba_bash',
                               #- este es el obligatorio 
                               bash_command='python3 /opt/airflow/dags/prueba.py',
                               do_xcom_push=True)
    end = DummyOperator(task_id='fin')

#- 3 La tercera en con la plantilla jinja, eso en las siguientes paginas
start >> [prueba_python, prueba_bash] >> prueba_pull >> end