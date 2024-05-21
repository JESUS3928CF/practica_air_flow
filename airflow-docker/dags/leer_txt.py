from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

def leer_archivo_txt():
    try:
        # Ruta del archivo txt dentro del contenedor de Airflow
        archivo_txt = '/opt/airflow/external_data/archivo.txt'  # Reemplaza con la ruta correcta
        
        # Comprobamos si el archivo existe
        if os.path.exists(archivo_txt):
            # Leemos el contenido del archivo
            with open(archivo_txt, 'r') as file:
                contenido = file.read()
                print("Contenido del archivo:")
                print(contenido)
        else:
            print("El archivo no existe en la ruta especificada.")
    except Exception as e:
        print(f"Error al leer el archivo: {str(e)}")

# Define el DAG
dag = DAG(
    'leer_archivo_txt_fin',
    description='Lee el contenido de un archivo TXT desde un volumen montado',
    schedule_interval=None,
    start_date=datetime(2024, 5, 20),
    catchup=False,
    tags=['lectura-archivos']
)

# Tarea para leer el archivo txt
leer_archivo_task = PythonOperator(
    task_id='leer_archivo_task',
    python_callable=leer_archivo_txt,
    dag=dag,
)

# Define las dependencias entre tareas (si es necesario)
leer_archivo_task

if __name__ == "__main__":
    dag.cli()
