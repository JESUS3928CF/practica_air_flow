from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

dag_owner = 'Jesus'

# Parámetros que podemos recibir desde airflow
params = {'Manual': True}

default_args = {
    'owner': dag_owner,
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='TEST_AUTO',
    default_args=default_args,
    description='',
    start_date=datetime(2024, 5, 15),
    schedule_interval=None,
    catchup=False,
    tags=['DeveloperJesus'],
    params=params
) as dag:

    start = EmptyOperator(task_id='start')

    @task
    def execute_task(**kwargs):
        print(kwargs)
        params = kwargs.get('params', {})
        manual = params.get('Manual', False)

        if manual:
            kwargs['ti'].xcom_push(key='color', value='Amarillo')
        else:
            kwargs['ti'].xcom_push(key='color', value='Rojo')

    @task
    def context_task(**kwargs):
        ti = kwargs['ti']
        color = ti.xcom_pull(task_ids='execute_task')
        print(color)

    end = EmptyOperator(task_id='end')

    # Create task instances
    execute_task_instance = execute_task()
    context_task_instance = context_task()

    # Set task dependencies
    start >> execute_task_instance >> context_task_instance >> end
