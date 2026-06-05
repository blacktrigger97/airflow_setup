FROM apache/airflow:slim-3.1.4rc2-python3.11

USER root

RUN mkdir -p /mnt/devCode

USER airflow

RUN pip3 install --upgrade pip
RUN pip3 install apache-airflow apache-airflow-providers-apache-spark psycopg2-binary
# pyspark==3.5.6
