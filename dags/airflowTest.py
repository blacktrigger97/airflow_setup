from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import platform
import logging

def print_worker_hostname():
    """A simple task to log the hostname of the executing worker."""
    hostname = platform.node()
    logging.info(f"Hello from Airflow worker on host: {hostname}")
    return hostname

with DAG(
    dag_id='celery_worker_test',
    start_date=days_ago(1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['test', 'celery']
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=print_worker_hostname,
    )

    for i in range(5):
        task = PythonOperator(
            task_id=f'task_{i+1}',
            python_callable=print_worker_hostname,
            # Optional: specify a queue if you have multiple worker queues
            # queue='my_dedicated_queue', 
        )
        start_task >> task

if __name__ == "__main__":
    dag.test()