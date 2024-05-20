# Importamos las clases necesarias de Airflow
from airflow import DAG  # Importa la clase DAG para definir un flujo de trabajo
from airflow.operators.mysql_operator import MySqlOperator  # Importa el operador MySqlOperator para ejecutar consultas MySQL
from airflow.utils.dates import days_ago  # Importa la función days_ago para obtener la fecha actual menos un número de días
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Definimos los argumentos predeterminados para las tareas del DAG
default_args = {
    'owner': 'airflow',  # Propietario del DAG
    'depends_on_past': False,  # Si las ejecuciones de la tarea dependen del éxito de las ejecuciones anteriores
    'email_on_failure': False,  # Si se envía un correo electrónico en caso de fallo
    'email_on_retry': False,  # Si se envía un correo electrónico en caso de reintentos
    'retries': 1,  # Número de reintentos en caso de fallo
}

# Creamos una instancia de DAG con sus respectivos parámetros
dag = DAG(
    'example_mysql_dag',  # Nombre del DAG
    default_args=default_args,  # Argumentos predeterminados para las tareas
    description='A simple MySQL DAG',  # Descripción del DAG
    schedule_interval=None,  # Intervalo de programación del DAG (None para desactivar la programación)
    start_date=days_ago(1),  # Fecha de inicio del DAG (un día antes de la fecha actual)
    tags=["DeveloperJesus"]
)

# Creamos una tarea que ejecutará una consulta MySQL
run_mysql_query = MySqlOperator(
    task_id='run_mysql_query',  # Identificador único de la tarea
    mysql_conn_id='local_mysql_connection',  # ID de la conexión MySQL configurada en Airflow
    sql='SELECT * FROM llamadas_duplicadas;',  # Consulta MySQL a ejecutar
    dag=dag,  # Asociamos la tarea al DAG que acabamos de definir
)

# Operador vacío al inicio del DAG
start_operator = DummyOperator(task_id='start', dag=dag)

# Operador vacío al final del DAG
end_operator = DummyOperator(task_id='end', dag=dag)

# Función que imprime la información consultada de la base de datos
def print_query_result():
    # Aquí puedes agregar código para obtener e imprimir la información consultada
    print("Query result printed successfully!")

# Operador Python para ejecutar la función de impresión
print_query_info = PythonOperator(
    task_id='print_query_info',
    python_callable=print_query_result,
    dag=dag
)


# Conexión de los operadores
start_operator >> run_mysql_query >> print_query_info >> end_operator