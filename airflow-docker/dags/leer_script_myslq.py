from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from datetime import datetime, timedelta 

dag_owner = 'Jesus Cochero'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
        }

with DAG(dag_id='leer_script_MySql',
        default_args=default_args,
        description='',
        start_date=datetime.now(),
        schedule_interval=None,  #- indica que Airflow buscará plantillas Jinja en el directorio como SQL's
        catchup=False,
        tags=['DeveloperJesus'],
        template_searchpath=['/opt/airflow/external_data']
):

    start = EmptyOperator(task_id='start')

    #/ Asi creamos una tabla
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id='numeros',  # Nombre de la conexión MySQL configurada en Airflow
        sql='''
        CREATE TABLE IF NOT EXISTS test (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INT
        )
        ''',
        database='data_warehouse'  # Nombre de la base de datos donde se creará la tabla
    )

    #/ Asi leemos el script de nuestro volumen
    MySqlOperator_task = MySqlOperator(
        task_id='MySqlOperator_task',
        mysql_conn_id='numeros',
        sql='numeros.sql',
        database=None,
        )

    end = EmptyOperator(task_id='end')

    start >> create_table  >> MySqlOperator_task >> end