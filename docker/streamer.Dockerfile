FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN pip install --no-cache-dir pyspark==3.5.1 python-dotenv

COPY docker/spark-defaults.container.conf /opt/spark-conf/spark-defaults.conf
ENV SPARK_CONF_DIR=/opt/spark-conf

WORKDIR /repo
