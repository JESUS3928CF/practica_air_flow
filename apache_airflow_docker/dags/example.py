
#/1) Primero las librerías internar y estrenas de python
from builtins import range
from datetime import timedelta

#/ Luego las
from airflow.models import DAG #! Esta linea siempre tiene que estar

#/ Luego los operator que queramos utilizar 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

#/ Luego las otras utilidades de airflow como conexiones variables etc..
from airflow.utils.dates import days_ago


#/ 2) Luego los argumentos por defecto y variables
args = {
    'owner': 'Airflow',
    'start_date': days_ago(2), #- forma de especificar la primera ejecución, cuidad con esto, ejemplo si lo ponemos cada semana puede que la primera no se ejecute
}



#/ 3 definición del dag
dag = DAG(
    #* Estos 3 argumentos siempre necesarios
    dag_id='example_bash_operator01',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['example', 'DeveloperJesus']
)


#/ 4) La siguiente parte son los operadores, 
#- cada uno es un objeto que instancia un tipo determinado de operator 
run_this_last = DummyOperator(
    task_id='run_this_last', #* Nombre de la tarea que esa task representa y aparecerá en la interfaz 
    dag=dag,
)

# [START howto_operator_bash]
run_this = BashOperator(
    task_id='run_after_loop',
    bash_command='echo 1',
    dag=dag,
)
# [END howto_operator_bash]

run_this >> run_this_last

for i in range(3):
    task = BashOperator(
        task_id='runme_' + str(i),
        bash_command='echo "{{ task_instance_key_str }}" && sleep 1',
        dag=dag,
    )
    task >> run_this

# [START howto_operator_bash_template]
also_run_this = BashOperator(
    task_id='also_run_this',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)

#/ 5 La ultima parte de un dag seria la definición de el flujo o se que tareas le siguen acaules y tal 
# [END howto_operator_bash_template]
also_run_this >> run_this_last

if __name__ == "__main__":
    dag.cli()