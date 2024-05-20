from datetime import datetime

from airflow.models import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 

argumentos_por_defecto = {
    'owner': 'Jesus Cochero',
    'start_date' : datetime(2024, 5, 19, 19, 29, 4), #- Se especifica hasta los segundos en este ejemplo
}


#/ Aca definimos nuestras funciones python
def hello_world_loop():
    for palabra in ['Hello', 'World']:
        print(palabra)


with DAG(dag_id='dag_de_prueba',
         default_args=argumentos_por_defecto,
         schedule_interval='@once' #- esto es para que solo se ejecute una vez
         ) as dag: 
    start = DummyOperator(task_id='inicio')

    prueba_python = PythonOperator(task_id='prueba_python', 
                                   python_callable=hello_world_loop)
    
    prueba_bash = BashOperator(task_id='prueba_bash',
                               #- este es el obligatorio 
                               bash_command='echo "hola mundo"')
    end = DummyOperator(task_id='fin')


start >> [prueba_python, prueba_bash] >> end