from datetime import datetime

from airflow.models import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator 

argumentos_por_defecto = {
    'owner': 'Jesus Cochero',
    'start_date' : datetime(2024, 5, 19, 19, 29, 4),
}


def hello_world_loop():
    for palabra in ['Hello', 'World']:
        print(palabra)
    
    return 'Hola desde el python operator el return se guarda en el xcom'

#/ Asi usamos el contexto desde una plantilla 
templated_command = """
    echo "Usando una variable de entorno: ", {{ var.value.email }}
    echo "Usando una variable de entorno en formato json: ", {{ var.json.variables }}
    echo "Usando info guardad en el contexto ", {{ ti.xcom_pull(task_ids='prueba_python') }}
    echo "La fecha de ejecución es {{ ds }}"
    echo "La fecha de ejecución anterior fue {{ prev_ds }}"
    echo "La fecha de ejecución + 7 días es {{ macros.ds_add(ds, 7) }}"
"""

with DAG(dag_id='jinja',
         default_args=argumentos_por_defecto,
         schedule_interval='@once' 
         ) as dag: 
    start = DummyOperator(task_id='inicio')

    prueba_python = PythonOperator(task_id='prueba_python', 
                                   python_callable=hello_world_loop)
    
    prueba_bash = BashOperator(task_id='prueba_bash',
                               #/ El bash command es un template feel
                               bash_command=templated_command)
    end = DummyOperator(task_id='fin')


start >> [prueba_python, prueba_bash] >> end