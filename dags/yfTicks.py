from __future__ import annotations
import sys
# import pandas_market_calendars as mcal
from datetime import datetime
from time import sleep
from utils import jobdir_chng

from airflow.sdk import task, dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(dag_id="yfTicks", schedule='@daily', start_date=datetime(2026, 1, 3), catchup=False)
def install_and_use_module_dag():

    jobdir_chng()
    
    @task.virtualenv(
        task_id="mStatus",
        system_site_packages=False, # Set to True to access system packages (including Airflow)
        requirements=["pandas_market_calendars"], # Specify packages and versions
        # inherit_env=True
    )
    def mStatus():

        import pandas_market_calendars as mcal
        from datetime import datetime, date

        # Create the NSE calendar
        nse_calendar = mcal.get_calendar('XNSE')
        
        runCheck = {"run_flag" : False}

        # Define the day you want to check (e.g., today)
        today = date.today()
        
        is_trading_day = nse_calendar.valid_days(start_date=today, end_date=today, tz='Asia/Kolkata')

        if not is_trading_day.empty:
            schedule = nse_calendar.schedule(start_date=today, end_date=today, tz='Asia/Kolkata')
        
            while int((datetime.now() - schedule.iloc[0]['market_open'].to_pydatetime().replace(tzinfo=None)).total_seconds()) > 300:
                sleep(1)
                continue
            
            runCheck["run_flag"] = True
        
        return runCheck


    @task.virtualenv(
        task_id="Ticks_FastInfo",
        system_site_packages=False, # Set to True to access system packages (including Airflow)
        requirements=["pystrm"], # Specify packages and versions
        inherit_env=True
    )
    def isolated_tick_task(mthd: str, key: str, ti=None):
        # This code runs inside the new virtual environment

        fetch_runflag = ti.xcom_pull(task_ids="mStatus")
        
        if fetch_runflag["run_flag"]:
            import pystrm # type: ignore 
            from pystrm import main_function # type: ignore 

            print(f"Python version in venv: {sys.version}")
            print(f"pystrm version: {pystrm.__version__}")
            # ... your task logic here ...

            return main_function(mthd, key)


    @task
    def reRunDag(ti=None):

        fetch_runflag = ti.xcom_pull(task_ids="mStatus")

        if fetch_runflag["run_flag"]:
            trigger_next_run = TriggerDagRunOperator(
                task_id='rerun',
                trigger_dag_id='yfTicks',
                # conf="{{ runStatus.xcom_pull(task_ids='mStatus', key='return_value') }}",
                # only_if_dag_run_exists=True, # Use if you want to reuse existing runs
                # wait_for_completion=True, # Use if the current DAG should wait for the new one
                trigger_rule='all_success' # Default behavior
            )

            trigger_next_run

    runStatus = mStatus()
    isolated_tick_task('liveYfinanaceTick', 'Yfinance.FastInfo', runStatus) >> reRunDag(runStatus)
    
install_and_use_module_dag()