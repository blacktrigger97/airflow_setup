from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


@dag(
    start_date=datetime(2025,6,11),
    schedule='@daily',
    catchup=False,
    tags=['stock_market']
)

def stock_market():
    
    @task.sensor(poke_interval=5, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is not None
        return PokeReturnValue(is_done=condition)

    


    is_api_available()

stock_market() 