from __future__ import annotations
import os
import sys
from airflow.sdk import task, dag
# from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
from chngdir import dir_chng

# sys.path.append(os.path.abspath('/root/airflow/jobs'))

# from pystrmMain import main

@dag(schedule=None, start_date=datetime(2025, 12, 16), catchup=False)
def install_and_use_module_dag():

    dir_chng()

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

        # fastInfo = PythonOperator(dag=dag,
        #         task_id='fastInfo',
        #         provide_context=False,
        #         python_callable=main,
        #         op_args=['liveYfinanaceTick', 'Yfinance.FastInfo'],
        #     #    op_kwargs={'keyword_argument':'which will be passed to function'}
        #         )

        fastInfo = BashOperator(dag=dag,
                task_id='fastInfo',
                bash_command="python pystrmMain.py liveYfinanaceTick Yfinance.FastInfo",
                # op_args=['liveYfinanaceTick', 'Yfinance.FastInfo'],
            #    op_kwargs={'keyword_argument':'which will be passed to function'}
                )
        
        fastInfo

    my_isolated_task()

install_and_use_module_dag()