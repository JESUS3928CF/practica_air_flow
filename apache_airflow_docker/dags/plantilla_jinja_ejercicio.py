from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from jinja2 import Template

# Definir argumentos básicos del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 1,
}

# Crear el DAG
dag = DAG(
    'multi_task_templates_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

# Función que usaremos en el PythonOperator
def render_task_template(custom_field, **kwargs):
    task_instance = kwargs['task_instance']
    execution_date = kwargs['execution_date']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id

    # Crear una plantilla Jinja
    template = Template("""
    Task ID: {{ task_id }}
    DAG ID: {{ dag_id }}
    Execution Date: {{ execution_date }}
    Custom Field: {{ custom_field }}
    """)

    # Renderizar la plantilla con atributos de la tarea
    rendered_template = template.render(
        task_id=task_id,
        dag_id=dag_id,
        execution_date=execution_date,
        custom_field=custom_field
    )

    print(rendered_template)

# Crear tareas que usan la plantilla
tasks = []
for i in range(3):
    task = PythonOperator(
        task_id='render_task_template_{}'.format(i),
        provide_context=True,
        python_callable=render_task_template,
        op_kwargs={'custom_field': 'Campo personalizado {}'.format(i+1)},
        dag=dag,
    )
    tasks.append(task)

# Definir la secuencia de tareas (en este caso todas pueden ejecutarse en paralelo)
for task in tasks:
    task
