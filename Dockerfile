FROM apache/airflow:3.1.5

USER root

# 1. Install System Dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    procps \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.0


# 3. Set Global Environment Variables
# DYNAMIC JAVA_HOME: This command finds the path regardless of arm64 vs amd64
RUN JAVA_PATH=$(readlink -f /usr/bin/java | sed "s:/bin/java::") && \
    echo "export JAVA_HOME=$JAVA_PATH" >> /etc/environment

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${PATH}:${SPARK_HOME}/bin"

# 4. CRITICAL: Spark needs to know which Python to use
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER airflow

# 5. Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==${SPARK_VERSION} \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    apache-airflow-providers-apache-spark \
    kagglehub 