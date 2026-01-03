from __future__ import annotations
import sys
import pandas_market_calendars as mcal
from airflow.sdk import task, dag
from datetime import datetime, date
from time import sleep
from utils import jobdir_chng


# Create the NSE calendar
nse_calendar = mcal.get_calendar('XNSE')

@dag(schedule=None, start_date=datetime(2026, 1, 5), catchup=False)
def install_and_use_module_dag():

    jobdir_chng()

    @task
    def mStatus() -> dict[str, bool]:
        
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
    def isolated_tick_task(ti, mthd: str, key: str) -> None:
        # This code runs inside the new virtual environment

        fetch_runflag = ti.xcom_pull(task_ids="mStatus", key="return_value")
        
        if fetch_runflag["run_flag"]:
            import pystrm
            from pystrm import main_function

            print(f"Python version in venv: {sys.version}")
            print(f"pystrm version: {pystrm.__version__}")
            # ... your task logic here ...

            return main_function(mthd, key)
        else:
            return None

    runStatus = mStatus()
    isolated_tick_task(runStatus, 'liveYfinanaceTick', 'Yfinance.FastInfo')

install_and_use_module_dag()