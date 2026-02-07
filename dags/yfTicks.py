from __future__ import annotations
import pendulum

from airflow.sdk import task, dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

local_timezone = pendulum.timezone("Asia/Kolkata") 


@dag(dag_id="yfTicks", schedule='0 2 * * *', start_date=pendulum.datetime(2026, 1, 29, tz=local_timezone), catchup=False)
def install_and_use_module_dag():
    

    @task.virtualenv(
        task_id="mStatus",
        system_site_packages=False, # Set to True to access system packages (including Airflow)
        requirements=["pandas_market_calendars"], # Specify packages and versions
    )
    def mStatus():

        import logging
        import pandas_market_calendars as mcal
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from time import sleep

        local_tz = ZoneInfo("Asia/Kolkata")
        now = datetime.now(local_tz)
        today = now.date()
        logging.info(f"Now: {now.isoformat()}")
        logging.info(f"Today: {today}")

        run_flag = False

        nse_calendar = mcal.get_calendar('XNSE')

        is_trading_day = nse_calendar.valid_days(start_date=today, end_date=today, tz='Asia/Kolkata')

        if not is_trading_day.empty:
            schedule = nse_calendar.schedule(start_date=today, end_date=today, tz='Asia/Kolkata')
            # keep times timezone-aware and compare in the same tz
            open_time = schedule.iloc[0]['market_open'].to_pydatetime().astimezone(local_tz)

            time_diff = int((open_time - datetime.now(local_tz)).total_seconds())

            if datetime.now(local_tz) <= open_time:
                while time_diff > 300:
                    time_diff = int((open_time - datetime.now(local_tz)).total_seconds())
                    logging.info(f"Time difference : {time_diff}")
                    sleep(60)

                run_flag = True
        
        return run_flag
    

    @task.virtualenv(
        task_id="Yf_Ticks",
        system_site_packages=False, # Set to True to access system packages (including Airflow)
        requirements=["pystrm"] # Specify packages and versions
    )
    def isolated_tick_task(mthd: str, key: str, cfg_path: str, fetch_runflag: str):
        # This code runs inside the new virtual environment
        import logging
        import sys
        from ast import literal_eval

        try:
            flag = fetch_runflag
            # handle the common case where the XCom is a string literal
            if isinstance(fetch_runflag, str):
                try:
                    flag = literal_eval(fetch_runflag)
                except (ValueError, SyntaxError):
                    flag = fetch_runflag.strip().lower() in ("true", "1", "yes")

            if flag:
                import pystrm  # type: ignore
                from pystrm import main_function  # type: ignore

                logging.info(f"Python version in venv: {sys.version}")
                logging.info(f"pystrm version: {getattr(pystrm, '__version__', 'unknown')}")

                return main_function(mthd, key, cfg_path)
        except Exception as exc:
            logging.exception("Error running tick task: %s", exc)
            raise exc


    @task.virtualenv(
        task_id="Yf_Spark_Sink",
        system_site_packages=False, # Set to True to access system packages (including Airflow)
        requirements=["mynk_etl"] # Specify packages and versions
    )
    def isolated_tick_spark_task(mthd: str, key: str, cfg_path: str, fetch_runflag: str):
        # This code runs inside the new virtual environment
        import logging
        import sys
        from ast import literal_eval

        try:
            flag = fetch_runflag
            # handle the common case where the XCom is a string literal
            if isinstance(fetch_runflag, str):
                try:
                    flag = literal_eval(fetch_runflag)
                except (ValueError, SyntaxError):
                    flag = fetch_runflag.strip().lower() in ("true", "1", "yes")

            if flag:
                import mynk_etl  # type: ignore
                from mynk_etl import main_function  # type: ignore

                logging.info(f"Python version in venv: {sys.version}")
                logging.info(f"mynk_etl version: {getattr(mynk_etl, '__version__', 'unknown')}")

                return main_function(mthd, key, cfg_path)
        except Exception as exc:
            logging.exception("Error running tick task: %s", exc)
            raise exc
        

    @task.python
    def reRunDag(fetch_runflag: str):

        if fetch_runflag:
            trigger_next_run = TriggerDagRunOperator(
                task_id='rerun',
                trigger_dag_id='yfTicks',
                trigger_rule='all_success'
            )

            # Execute the operator from within the Python task so the DAG is triggered immediately
            trigger_next_run.execute()


    flag = mStatus()
    ticks = isolated_tick_task("liveYfinanaceTick", "Yfinance.FastInfo", "/root/airflow/jobs", flag) 
    spark_sink = isolated_tick_spark_task("yfinanceTickData", "Yfinance.FastInfo", "/root/airflow/jobs", flag) 
    ticks >> spark_sink >> reRunDag(flag)
    
install_and_use_module_dag()