from __future__ import annotations
import logging
import pendulum

from airflow.sdk import task, dag
from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

local_timezone = pendulum.timezone("Asia/Kolkata") 


@dag(dag_id="yfTicks", schedule='1 0 * * *', start_date=pendulum.datetime(2026, 1, 29, tz=local_timezone), catchup=False)
def install_and_use_module_dag():

    # jobdir_chng()
    
    def mStatus():

        import logging
        import pendulum
        import pandas_market_calendars as mcal
        from time import sleep
        now = pendulum.datetime(tz=local_timezone).now()
        today = now.date()
        logging.info(f"Now: {now.isoformat()}")
        logging.info(f"Today: {today}")

        run_flag = False

        nse_calendar = mcal.get_calendar('XNSE')

        is_trading_day = nse_calendar.valid_days(start_date=today, end_date=today, tz='Asia/Kolkata')

        if not is_trading_day.empty:
            schedule = nse_calendar.schedule(start_date=today, end_date=today, tz='Asia/Kolkata')
            # keep times timezone-aware and compare in the same tz
            open_time = schedule.iloc[0]['market_open'].to_pydatetime().astimezone(local_timezone)

            time_diff = int((open_time - pendulum.datetime(tz=local_timezone).now()).total_seconds())

            if pendulum.datetime(tz=local_timezone).now() <= open_time:
                while time_diff > 300:
                    time_diff = int((open_time - pendulum.datetime(tz=local_timezone).now()).total_seconds())
                    logging.info(f"Time difference : {time_diff}")
                    sleep(60)

                run_flag = True

        return run_flag
    
    
    runStatusCheck = PythonVirtualenvOperator(
        task_id='mStatus',
        python_callable=mStatus,
        do_xcom_push=True, # Must be True (default)
        requirements=['pandas_market_calendars'], 
        system_site_packages=True
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
            
        
    fastInfo = PythonVirtualenvOperator(
        task_id='Ticks_FastInfo',
        python_callable=isolated_tick_task,
        system_site_packages=True,
        requirements=['pystrm'],
        op_kwargs={
            "mthd" : "liveYfinanaceTick",
            "key" : 'Yfinance.FastInfo',
            "cfg_path" : "/root/airflow/jobs",
            # Use Jinja to render the XCom value into the argument
            "fetch_runflag": "{{ ti.xcom_pull(task_ids='mStatus', key='return_value') }}"
        }
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
            
        
    sparkFastInfo = PythonVirtualenvOperator(
        task_id='Ticks_Spark_FastInfo',
        python_callable=isolated_tick_spark_task,
        system_site_packages=True,
        requirements=['mynk_etl'],
        op_kwargs={
            "mthd" : "yfinanceTickData",
            "key" : "Yfinance.FastInfo",
            "cfg_path" : "/root/airflow/jobs",
            # Use Jinja to render the XCom value into the argument
            "fetch_runflag": "{{ ti.xcom_pull(task_ids='mStatus', key='return_value') }}"
        }
    )


    @task
    def reRunDag(**context):

        fetch_runflag = context["ti"].xcom_pull(task_ids="mStatus", key="return_value")

        logging.info(f"fetch_runflag : {fetch_runflag}")

        if fetch_runflag:
            trigger_next_run = TriggerDagRunOperator(
                task_id='rerun',
                trigger_dag_id='yfTicks',
                trigger_rule='all_success'
            )

            # Execute the operator from within the Python task so the DAG is triggered immediately
            trigger_next_run.execute(context)

    runStatusCheck >> fastInfo >> sparkFastInfo >> reRunDag()
    
install_and_use_module_dag()