# Practica Airflow

Este repositorio contiene ejemplos y ejercicios para aprender y practicar el uso de Apache Airflow. Está diseñado para ayudar a comprender los conceptos básicos y avanzados de la orquestación de flujos de trabajo utilizando Airflow.

## Descripción

El objetivo de este proyecto es proporcionar una colección de ejemplos prácticos y ejercicios que cubren diversas características y funcionalidades de Apache Airflow. Comenzamos con un DAG básico y planeamos agregar más ejemplos y casos de uso complejos a medida que avancemos.

## Ejemplo incluido principal

### mi_primer_dag.py

Un DAG simple que incluye:
- `start_task`: Una tarea vacía que marca el inicio del flujo de trabajo.
- `first_task`: Una tarea Python que imprime "Hola Mundo" en la consola.
- `end_task`: Una tarea vacía que marca la finalización del flujo de trabajo.

## Requisitos

Para ejecutar este proyecto, necesitas tener instalado lo siguiente:
- Python 3.6+
- Apache Airflow

## Instalación

1. Clona este repositorio en tu máquina local:
    ```sh
    git clone https://github.com/tu_usuario/practica_air_flow.git
    cd practica_air_flow
    ```

2. Crea y activa un entorno virtual (opcional pero recomendado):
    ```sh
    python3 -m venv venv
    source venv/bin/activate  # En Windows usa `venv\Scripts\activate`
    ```

3. Instala las dependencias requeridas:
    ```sh
    pip install apache-airflow
    ```

## Configuración

1. Copia el archivo DAG a tu directorio de DAGs de Airflow. Por ejemplo:
    ```sh
    cp mi_primer_dag.py ~/airflow/dags/
    ```

2. Inicia el servidor web de Airflow:
    ```sh
    airflow webserver
    ```

3. En una nueva terminal, inicia el scheduler de Airflow:
    ```sh
    airflow scheduler
    ```

4. Abre el panel de control de Airflow en tu navegador:
    ```
    http://localhost:8080
    ```

5. Activa el DAG llamado `MI_PRIMER_DAG` en la interfaz de Airflow.

## Uso

Una vez que el DAG esté activado, Airflow ejecutará las tareas según el cronograma definido (todos los días a las 7:42 PM). Puedes observar el progreso y los registros de las tareas directamente en la interfaz de usuario de Airflow.

## Ejercicios y Ejemplos Fututos

Planeamos agregar más ejemplos que cubrirán:
- Tareas dependientes y paralelismo
- Manejo de errores y reintentos
- Integración con bases de datos y APIs externas
- Ejecución de scripts Bash y otros operadores
- Sensores y dinámicas avanzadas de programación de tareas

## Contribución

Si deseas contribuir a este proyecto, por favor, sigue estos pasos:

1. Haz un fork de este repositorio.
2. Crea una nueva rama (`git checkout -b feature/nueva-funcionalidad`).
3. Realiza tus cambios y haz commit (`git commit -am 'Añade nueva funcionalidad'`).
4. Empuja la rama (`git push origin feature/nueva-funcionalidad`).
5. Abre un Pull Request.

## Licencia

Este proyecto está bajo la Licencia MIT. Para más detalles, consulta el archivo `LICENSE`.

## Contacto

Si tienes alguna pregunta o sugerencia, por favor, contacta a [Jesús Cochero](mailto:jesus3928cf@gmail.com).
