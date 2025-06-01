from airflow.decorators import dag, task
from datetime import datetime


def task_a():
    print("Task A")
    return 42


@dag(
    start_date=datetime(2025,6,1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market_status']
)

def stock_market():
    
    @task
    def task_a():
        print("Task A")
        return 42

    @task
    def task_b(value):
        print("Task B")
        print(value)


    task_b(task_a())

stock_market() 