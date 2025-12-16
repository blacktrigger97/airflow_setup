from __future__ import annotations
import sys
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from datetime import datetime

@dag(schedule_interval=None, start_date=datetime(2025, 12, 16), catchup=False)
def install_and_use_module_dag():

    @task.virtualenv(
        task_id="Ticks",
        requirements=["pystrm"], # Specify packages and versions
        system_site_packages=False, # Set to True to access system packages (including Airflow)
    )
    def my_isolated_task():
        # This code runs inside the new virtual environment
        import pystrm
        
        print(f"Python version in venv: {sys.version}")
        print(f"pystrm version: {pystrm.__version__}")
        # ... your task logic here ...

        fastInfo = PythonOperator(dag=dag,
                task_id='fastInfo',
                provide_context=False,
                python_callable=pystrm,
                op_args=['liveYfinanaceTick', 'Yfinance.FastInfo'],
            #    op_kwargs={'keyword_argument':'which will be passed to function'}
                )
        
    my_isolated_task()

install_and_use_module_dag = install_and_use_module_dag()

