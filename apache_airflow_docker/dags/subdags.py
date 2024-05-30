from datetime import datetime

from airflow.models import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator

#/ Importación necesaria
from airflow.operators.subdag_operator import SubDagOperator
#* los sub dag no tienen encuesta las pools y puede pasar que un subdag se como todos los recursos que tenemos para ejecutar todas als tareas
#* 2) Este bug fue solucionado pero hay que hacer lo siguiente para que pueda ejecutar tareas en paralelo importar el executor que queremos ponerle
from airflow.executors.local_executor import LocalExecutor

default_args = {
    'owner': 'Jesus Cochero',
    'start_date' : datetime(2024, 5, 19, 19, 29, 4),
}

#/ Nombres de los dag
PARENT_DAG_NAME = 'sud_dag_padre'
#- Al parecer el nombre de el dag hijo debe de ser el nombre del padre mas . nombre del operador hijo
CHILD_DAG_NAME = PARENT_DAG_NAME + '.sub_dag_1'

#/ CREANDO EL SUB DAG, 
#- Tiene que recibir 3 argumentos
def load_sub_dag(parent_dag_name, child_dag_name, default_args): #* Aun que no utilizamos parent_dag_name hay que pasarlo para que se ejecute

    #/ 1) Definimos el subdag
    with DAG(child_dag_name,
             default_args=default_args,
             #- Poner el mismo schedule_interval que en parent dag por que si no dire que la tarea se realizo con éxito
             schedule_interval='@daily' ) as sub_dag:

            #/ 2 Definimos las tareas
            start = DummyOperator(task_id='inicio')

            end = DummyOperator(task_id='fin')
        
            #/ 3 definimos las dependencias
            start >> end

            #/4 Por ultimo devolvemos el subdag que hemos creado
            return sub_dag

with DAG(dag_id=PARENT_DAG_NAME, #-
         default_args=default_args,
         schedule_interval='@daily' 
         ) as dag: 
    start = DummyOperator(task_id='inicio')

    prueba_python = PythonOperator(task_id='prueba_python', 
                                   python_callable=load_sub_dag)
    
    sub_dag_1 = SubDagOperator(
         task_id='sub_dag_1',
         subdag=load_sub_dag(PARENT_DAG_NAME, CHILD_DAG_NAME, dag.default_args), #- asi cejemos los parámetros por defecto 
         executor=LocalExecutor() #/ 2) Asi ejecutamos en paralelo las funciones
    )
    
    end = DummyOperator(task_id='fin')


start >> prueba_python >> end