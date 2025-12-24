from __future__ import annotations
import sys
from airflow.sdk import task, dag
from datetime import datetime
from chngdir import jobdir_chng


@dag(schedule=None, start_date=datetime(2025, 12, 16), catchup=False)
def install_and_use_module_dag():

    jobdir_chng()

    @task.virtualenv(
        task_id="Ticks",
        system_site_packages=False, # Set to True to access system packages (including Airflow)
        requirements=["pystrm"], # Specify packages and versions
        inherit_env=True
    )
    def isolated_tick_task(mthd: str, key: str):
        # This code runs inside the new virtual environment
        
        import pystrm
        from pystrm import main_function

        print(f"Python version in venv: {sys.version}")
        print(f"pystrm version: {pystrm.__version__}")
        # ... your task logic here ...

        return main_function(mthd, key)

    isolated_tick_task('liveYfinanaceTick', 'Yfinance.FastInfo')

install_and_use_module_dag()