
#/ 1) importar las librerías necesarias 
from datetime import datetime, timedelta #- librerías para poder trabajar con fechas
#- de la librería airflow importaremos DAG y un par de operadores
from airflow import DAG 
from airflow.operators.empty import EmptyOperator #* Este es el operador para crear tareas dummy  
from airflow.operators.python import PythonOperator #* Este es para ejecutar codigo python

#/ 2) luego declararemos variables básicas que necesitaremos
 
TAGS = ['DeveloperJesus'] #un tag que nos permitirá filtrarlo después
DAG_ID = "MI_PRIMER_DAG" # NOMBRE EN EL QUE SALE EN EL AMBIENTE
DAG_DESCRIPTION = """ MI_PRIMER_DAG para el curso de airflow """
DAG_SCHEDULE = "14 04 * * *"  # Un intervalo de tiempo Todos los días a las 7:42 PM 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 13), 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 4, 
    'retry_delay': timedelta(minutes=5), 

    
}

#* estableemos los reintentos
retries = 4# numero de reintentos
retry_delay = timedelta(minutes=5) # el periodo te tiempo que va a esperar para rehacer cada reintento 


#/ 3) creamos el un método para imprimir el consola 
def execute_task ():
    print("Hola Mundo")

# Crear el objeto DAG forma 2 corruptor estándar
dag = DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    catchup=False, # esto lo que hace es que el dag se ponga al dia con todas las ejecuciones que no se han realizado desde el star day
    schedule_interval=DAG_SCHEDULE,
    max_active_runs=1, # maxima cantidad de ejecuciones que puede tener el dag al tiempo 
    dagrun_timeout=200000, # Establecer el límite de tiempo máximo
    default_args=default_args,
    tags=TAGS
)


#/ 4 crear las tareas que irán asociadas a ese dag
with dag as dag:
    #* esta tarea es un operador dummy que no hace nada solo es una bandera que indica cuando el proceso inicio
    start_task = EmptyOperator(
        task_id='start_task',
    )

    #* marca la finalización
    end_task = EmptyOperator(
        task_id='end_task',
    )

    #* asi luce una tarea tipo python operator que esta si va a realizar una tarea especifica que sera imprimir nuestro hola mundo
    first_task = PythonOperator(
        task_id='first_task',
        python_callable=execute_task,
        retries=retries,
        retry_delay=retry_delay,
    )


#/ 5 establecer las dependencias entre cada uno de las tareas
# >> esto es para establecer dependencia, en este ejemplo first task depende de start_task y end de first
start_task >> first_task >> end_task # esto lo que hace es que las tareas se ejecuten en serie 