from airflow.decorators import dag, task
from datetime import datetime


@dag(
    start_date=datetime(2025,5,31),
    schedule='@daily',
    cathup=False,
    tags=['stock_market_status']
)

def stock_market():
    pass

stock_market()