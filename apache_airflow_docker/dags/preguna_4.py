from airflow import DAG
from airflow.utils import dates 
from airflow.operators.bash_operator import  BashOperator

default_args = {
    'start_date' : dates.days_ago(1)
}

with DAG(
    'variable_plantilla_macro',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily'
) as dag:
    variable = BashOperator(task_id='variable',
                            bash_command='echo {{ var.value.email }}, {{ var.value.variables }}')