# Usa la imagen base de Apache Airflow
FROM apache/airflow:1.10.10

# Usuario root para poder instalar paquetes
USER root

# Eliminar el repositorio MySQL no confiable y actualizar las fuentes
RUN sed -i '/repo.mysql.com/d' /etc/apt/sources.list.d/* && \
    apt-get update && \
    apt-get install -y python3-tk xvfb && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Vuelve a cambiar al usuario airflow
USER airflow