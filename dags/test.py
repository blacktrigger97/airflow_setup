import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator


@dag(start_date=datetime.datetime(2025, 6, 8), catchup=False, schedule="@daily", tags=['testing'])
def generate_dag():
    EmptyOperator(task_id="task")


generate_dag()