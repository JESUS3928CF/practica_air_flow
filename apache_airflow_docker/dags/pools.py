from datetime import datetime

from airflow.models import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 

argumentos_por_defecto = {
    'owner': 'Jesus Cochero',
    'start_date' : datetime(2024, 5, 19, 19, 29, 4),
    #! TAREAS PRIORITARIAS
    #- Pueden haber 120 tareas más ejecutándose que nosotros seguiremos teniendo recursos para estas tareas
}


#/ Aca definimos nuestras funciones python
def hello_world_loop():
    for palabra in ['Hello', 'World']:
        print(palabra)


with DAG(dag_id='pools',
         default_args=argumentos_por_defecto,
         schedule_interval='@once'
         ) as dag: 
    start = DummyOperator(task_id='inicio')

    #/ Si lo prioritario es solo una tarea  del dag la quitamos de los argumentos por defecto y lo ponemos aca
    prueba_python = PythonOperator(task_id='prueba_python', 
                                   python_callable=hello_world_loop,
                                   pool='prioridad') #- Asi 
    
    prueba_bash = BashOperator(task_id='prueba_bash',
                               #- este es el obligatorio 
                               bash_command='echo "hola mundo"')
    end = DummyOperator(task_id='fin')


start >> [prueba_python, prueba_bash] >> end