FROM apache/airflow:2.8.2-python3.11

USER root
# Instalar OpenJDK para soporte de tareas Spark y utilidades
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow

# Dependencias necesarias para conectores, MinIO S3 y Kafka
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-apache-spark==4.7.0 \
    boto3==1.34.62 \
    kafka-python==2.0.2 \
    pydantic==2.6.4 \
    requests==2.31.0 \
    psycopg2-binary==2.9.9
