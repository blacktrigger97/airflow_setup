import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="Mayank_Test",
    start_date=datetime.datetime(2025, 6, 8),
    catchup=False,
    schedule="@daily",
):
EmptyOperator(task_id="task")