from airflow import DAG 
from airflow.utils import dates 
from airflow.operators.bash_operator import BashOperator 

argumentos_por_defecto = {
    'owner': 'Jesus Cochero',
    'start_date' : dates.days_ago(2),
}

