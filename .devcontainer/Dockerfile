FROM apache/airflow:slim-3.1.4rc2-python3.11

USER root

RUN mkdir -p /mnt/devCode 

USER airflow

RUN pip3 install --upgrade pip
RUN pip3 install apache-airflow apache-airflow-providers-apache-spark pyspark==3.5.6 psycopg2-binary

