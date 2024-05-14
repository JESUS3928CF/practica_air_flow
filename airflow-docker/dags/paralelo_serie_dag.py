from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Variables básicas
TAGS = ['DeveloperJesus']
DAG_ID = "TAREAS_EN_PARALELO_Y_EN_SERIE"
DAG_DESCRIPTION = "Ejecutar tareas en paralelo y en serie"
DAG_SCHEDULE = "42 19 * * *"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=5),
}

def execute_task(task_name):
    print(f"Ejecutando {task_name}")

dag = DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    catchup=False,
    schedule_interval=DAG_SCHEDULE,
    max_active_runs=1,
    dagrun_timeout=timedelta(seconds=200000),
    default_args=default_args,
    tags=TAGS
)

with dag:
    start_task = EmptyOperator(task_id='start_task')

    first_task = PythonOperator(
        task_id='first_task',
        python_callable=lambda: execute_task('first_task'),
    )

    second_task = PythonOperator(
        task_id='second_task',
        python_callable=lambda: execute_task('second_task'),
    )

    third_task = PythonOperator(
        task_id='third_task',
        python_callable=lambda: execute_task('third_task'),
    )

    four_task = PythonOperator(
        task_id='four_task',
        python_callable=lambda: execute_task('four_task'),
    )

    five_task = PythonOperator(
        task_id='five_task',
        python_callable=lambda: execute_task('five_task'),
    )

    end_task = EmptyOperator(task_id='end_task')

    # Definir las dependencias
    start_task >> [first_task, second_task] >> third_task >> [four_task, five_task] >> end_task
