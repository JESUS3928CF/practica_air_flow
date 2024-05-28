from airflow import DAG
from airflow.utils import dates 
from airflow.operators.python_operator import PythonOperator 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date' : dates.days_ago(1)
}

def guardar_fecha(**context):
        #/ esto es teoría por que no tenemos una db de postgres
        """ postgres = PostgresHook('normal_redshift')
        fecha_max_df = postgres.get_pandas_df('select max(load_datetime) from users')

        fecha_max_list = fecha_max_df.tolist()
        fecha_max = fecha_max_list[0] """


        ti = context['task_instance']
        ti.xcom_push(key='fecha', value='2025-01-01')

with DAG('dag_solucion_tarea_jinja',
          default_args=default_args,
          schedule_interval='@daily') as dag:

        obtener_fecha = PythonOperator(task_id = 'obtener_fecha',
                                       python_callable=guardar_fecha,
                                       provide_context=True)
        

        imprimir_fecha = BashOperator(task_id='imprimir_fecha',
                                      bash_command='echo {{ ti.xcom_pull(task_ids="obtener_fechas", key="fecha") }}'
                                      )
        

