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
RUN JAVA_PATH=$(readlink -f /usr/bin/java | sed "s:/bin/java::") && \
    echo "export JAVA_HOME=$JAVA_PATH" >> /etc/environment

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${PATH}:${SPARK_HOME}/bin"

# 4. Ensure Python
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

USER airflow

# 5. Install Python dependencies
RUN pip install -r requirements.txt