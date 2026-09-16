FROM apache/airflow:3.0.2

USER root
# PySpark needs a JVM
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    dbt-core \
    "dbt-spark[session]>=1.8" \
    psycopg2-binary \
    python-dotenv \
    mlflow==2.14.1 \
    lightgbm \
    scikit-learn \
    numpy \
    pandas

# every Spark session inside this image inherits the container-hostname conf
COPY docker/spark-defaults.container.conf /opt/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/spark-conf
