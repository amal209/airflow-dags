from asyncio import Task
from datetime import date
from email.errors import CloseBoundaryNotFoundDefect

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
import yfinance 
import numpy as np
import pandas as pd



default_args = {
    'retries':2
}

def extractData(ti):
    table="btc_price"
    #calling Yahoo finance API and requesting to get data for the last 22 hours, with an interval of 15 minutes.
    data = yfinance.download(tickers='BTC-USD', period = '22h', interval = '15m')
    data
    print('Data extracted')
    print(data)
    ti.xcom_push(key='my_data', value=data)

with DAG(
    'BTC_Price',
    default_args=default_args,
    description='Getting the BTC price from Yahoo',
    #schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 4, 13, tz="UTC"),
) as dag:
    # task1 ==> extract data

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extractData,
        dag=dag,
    )

    '''
    # task2 ===>  create table
    create_table = PostgresOperator(
        task_id="create_table_btc_price",
        postgres_conn_id='airflow-postgresql',
        sql="""
            CREATE TABLE IF NOT EXISTS btc_price (
            id SERIAL PRIMARY KEY,
            Datetime DATE NOT NULL,
            Open FLOAT NOT NULL,
            High FLOAT NOT NULL,
            Low FLOAT NOT NULL,
            Close FLOAT NOT NULL);
          """,
    )'''






#Order of tasks 
extract_data