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

# Dependencias necesarias para conectores, MinIO S3 y Kafka.
# pyspark se fija a 3.5.1 para coincidir con el clúster Spark del compose;
# sin el pin, pip instala 4.x y el cliente deja de ser compatible.
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-apache-spark==4.7.0 \
    boto3==1.34.62 \
    kafka-python==2.0.2 \
    pydantic==2.6.4 \
    requests==2.31.0 \
    psycopg2-binary==2.9.9 \
    pyspark==3.5.1

# El wheel de PySpark no trae los conectores S3A de Hadoop. El driver de
# Airflow los necesita para leer `s3a://market-lake/...` al reconciliar.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
RUN curl -fsSL -o "${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
    && ls -la "${SPARK_HOME}/jars/" | grep -E "hadoop-aws|aws-java-sdk-bundle"
