from datetime import datetime

from airflow.models import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 

argumentos_por_defecto = {
    'owner': 'Jesus Cochero',
    'start_date' : datetime(2024, 5, 19),
}

with DAG(dag_id='tareas_dinamicas',
         default_args=argumentos_por_defecto,
         schedule_interval='@once'
         ) as dag: 
    
    operators_list = []

    start = DummyOperator(task_id='inicio')

    operators_list.append(start)

    #/ Definir las tareas dinamicas
    for pais in ['Francia', 'Italia', 'Alemania', 'Uk']:
        imprimir  = BashOperator(
            task_id=f"imprimir_{pais}",
            bash_command=f'echo {pais}',
        )

        operators_list.append(imprimir)
    
    end = DummyOperator(task_id='fin')

    operators_list.append(end)

#/ FOR LOOP para recorrer las tareas y asignar dependencia
for i in range(len(operators_list) - 1):

    operators_list[i] >> operators_list[i + 1]

     #/ De forma explicita nuestra lista contendría los siguientes elementos
     #- No es confuso porque cada imprimir tiene un id único entonces ya se sabe cual tarea es cual
    #[start, imprimir, imprimir, imprimir, imprimir,  end]