from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta 

dag_owner = 'Jesus Cochero'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2)
        }

#/ Aca definimos los parámetros
params = {
    'ENV': 'DEV',
    'ID': '1234567890',
    'Manual': False
}

#/ el **kwargs es para poder acceder al contexto del dag
def execute_task (**kwargs):
    print(kwargs)

with DAG(dag_id='contexto_dag',
        default_args=default_args,
        description='usando el contexto de las tareas',
        start_date=datetime.now(),
        schedule_interval=None,
        catchup=False,
        tags=['DeveloperJesus']
):

    start = EmptyOperator(task_id='start')

    python_task = PythonOperator(
        task_id="python_task",
        python_callable=lambda: print('Hi from python operator'),
        # op_kwargs: Optional[Dict] = None,
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )

    end = EmptyOperator(task_id='end')

    start >> task_1() >> end