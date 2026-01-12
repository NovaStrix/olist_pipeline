FROM apache/airflow:3.1.5

USER root

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    procps \
    ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Spark
ENV SPARK_VERSION=3.5.7

RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mkdir -p /opt/spark && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark/spark-${SPARK_VERSION} && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Force Spark to use Java 17
RUN echo "export JAVA_HOME=${JAVA_HOME}" > /opt/spark/conf/spark-env.sh

# PySpark runtime
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER airflow

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir pyspark==3.5.0

# Runtime dirs
RUN mkdir -p \
    /opt/airflow/data/raw \
    /opt/airflow/data/processed \
    /opt/airflow/data/output \
    /opt/airflow/src \
    /tmp/spark-data

# Final verification
RUN java -version \
    && spark-submit --version \
    && python -c "import pyspark; print('PySpark', pyspark.__version__)"

WORKDIR /opt/airflow
