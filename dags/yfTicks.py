from __future__ import annotations
import sys
from datetime import datetime
from utils import jobdir_chng

from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(dag_id="yfTicks", schedule='@daily', start_date=datetime(2026, 1, 3), catchup=False)
def install_and_use_module_dag():

    jobdir_chng()
    
    def mStatus():

        import logging
        import pandas_market_calendars as mcal
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from time import sleep

        local_tz = ZoneInfo("Asia/Kolkata")
        today = datetime.now(local_tz).date() 
        logging.info(datetime.now(local_tz).replace(tzinfo=None))
        logging.info(f"Today : {today}")

        runCheck = {"run_flag" : False}

        nse_calendar = mcal.get_calendar('XNSE')
        
        is_trading_day = nse_calendar.valid_days(start_date=today, end_date=today, tz='Asia/Kolkata')

        if not is_trading_day.empty:
            schedule = nse_calendar.schedule(start_date=today, end_date=today, tz='Asia/Kolkata')
            open_time = schedule.iloc[0]['market_open'].to_pydatetime().replace(tzinfo=None)
            
            time_diff = int((open_time - datetime.now(local_tz).replace(tzinfo=None)).total_seconds())

            if datetime.now(local_tz).replace(tzinfo=None) <= open_time:
                while (time_diff > 300):
                    time_diff = int((open_time - datetime.now(local_tz).replace(tzinfo=None)).total_seconds())
                    logging.info(f"Time difference : {time_diff}")
                    sleep(60)
                    continue
                
                runCheck["run_flag"] = True
        
        return runCheck["run_flag"]
    
    
    runStatusCheck = PythonVirtualenvOperator(
        task_id='mStatus',
        python_callable=mStatus,
        do_xcom_push=True, # Must be True (default)
        requirements=['pandas_market_calendars'], 
        system_site_packages=True
    )


    def isolated_tick_task(mthd: str, key: str, fetch_runflag: str):
        # This code runs inside the new virtual environment

        from ast import literal_eval
        from time import sleep

        try:
            flag = literal_eval(fetch_runflag)

            if flag:
                import pystrm # type: ignore 
                from pystrm import main_function # type: ignore 

                print(f"Python version in venv: {sys.version}")
                print(f"pystrm version: {pystrm.__version__}")

                return main_function(mthd, key)
        except (ValueError, SyntaxError):
            print(f"Error: '{fetch_runflag}' is not a valid Python literal")
            sleep(1)
            sys.exit(1)
            
        
    fastInfo = PythonVirtualenvOperator(
        task_id='Ticks_FastInfo',
        python_callable=isolated_tick_task,
        system_site_packages=False,
        requirements=['pystrm'],
        op_kwargs={
            "mthd" : "liveYfinanaceTick",
            "key" : 'Yfinance.FastInfo',
            # Use Jinja to render the XCom value into the argument
            "fetch_runflag": "{{ ti.xcom_pull(task_ids='mStatus', key='return_value') }}"
        }
    )


    @task
    def reRunDag(**context):

        fetch_runflag = context["ti"].xcom_pull(task_ids="mStatus", key="return_value")

        if fetch_runflag:
            trigger_next_run = TriggerDagRunOperator(
                task_id='rerun',
                trigger_dag_id='yfTicks',
                trigger_rule='all_success'
            )

            trigger_next_run

    runStatusCheck >> fastInfo >> reRunDag()
    
install_and_use_module_dag()