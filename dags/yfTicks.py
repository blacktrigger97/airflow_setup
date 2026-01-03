from __future__ import annotations
import sys
from datetime import datetime
from time import sleep
from utils import jobdir_chng

from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(dag_id="yfTicks", schedule='@daily', start_date=datetime(2026, 1, 3), catchup=False)
def install_and_use_module_dag():

    jobdir_chng()
    
    @task
    def mStatus(**context):

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
        
        context["ti"].xcom_push(key="run_flag", value=runCheck["run_flag"])


    def isolated_tick_task(mthd: str, key: str, fetch_runflag: str):
        # This code runs inside the new virtual environment

        from ast import literal_eval

        try:
            flag = literal_eval(fetch_runflag)
            # result is False and has type <class 'bool'>
        except (ValueError, SyntaxError):
            # Handle cases where the string isn't a valid Python literal
            print(f"Error: '{fetch_runflag}' is not a valid Python literal")

        if flag:
            import pystrm # type: ignore 
            from pystrm import main_function # type: ignore 

            print(f"Python version in venv: {sys.version}")
            print(f"pystrm version: {pystrm.__version__}")
            # ... your task logic here ...

            return main_function(mthd, key)
        
    fastInfo = PythonVirtualenvOperator(
        task_id='Ticks_FastInfo',
        python_callable=isolated_tick_task,
        system_site_packages=False,
        requirements=['pystrm'],
        op_kwargs={
            "mthd" : "liveYfinanaceTick",
            "key" : 'Yfinance.FastInfo',
            # Use Jinja to render the XCom value into the argument
            "fetch_runflag": "{{ ti.xcom_pull(task_ids='mStatus', key='run_flag') }}"
        }
    )


    @task
    def reRunDag(**context):

        fetch_runflag = context["ti"].xcom_pull(task_ids="mStatus", key="run_flag")

        if fetch_runflag:
            trigger_next_run = TriggerDagRunOperator(
                task_id='rerun',
                trigger_dag_id='yfTicks',
                trigger_rule='all_success' # Default behavior
            )

            trigger_next_run

    mStatus() >> fastInfo >> reRunDag()
    
install_and_use_module_dag()