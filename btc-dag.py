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

from airflow.hooks.postgres_hook import PostgresHook 
#from airflow.hooks.postgres_hook.PostgresHook import get_conn




default_args = {
    'retries':2
}

# extract data
def extract():
    table="btc_price"
    #calling Yahoo finance API and requesting to get data for the last 22 hours, with an interval of 15 minutes.
    data = yfinance.download(tickers='BTC-USD', period = '22h', interval = '15m')
    data
    print('Data extracted')
    #print(data)
    data.to_csv("/tmp/data.csv", index=True)

#transform data
def transform():
    # read csv
    data = pd.read_csv("/tmp/data.csv")
    print("reading csv",data)

    # keep only 5 columns
    price_df = pd.DataFrame(data , columns = ["Datetime" , "Open", "High", "Low" , "Close"])
    print("PRICE DATAFRAME : ",price_df)
    price_df.to_csv("/tmp/price_df.csv", index=True)

# create table 
def create_table():#(query): 
    postgres = PostgresHook(postgres_conn_id='airflow-postgresql') 
    conn = postgres.get_conn() 
    cursor = conn.cursor()
    #sql=""" CREATE TABLE IF NOT EXISTS btc_price (id SERIAL PRIMARY KEY, Datetime DATE NOT NULL, Open FLOAT NOT NULL, High FLOAT NOT NULL, Low FLOAT NOT NULL,Close FLOAT NOT NULL);"""
    cursor.execute("CREATE TABLE IF NOT EXISTS btc_price (id SERIAL PRIMARY KEY, Datetime DATE UNIQUE NOT NULL, Open FLOAT NOT NULL, High FLOAT NOT NULL, Low FLOAT NOT NULL, Close FLOAT NOT NULL);")
    conn.commit()

# load data
def load():
    #get data
    price_df = pd.read_csv("/tmp/price_df.csv")
    # parametrs of the connection
    postgres = PostgresHook(postgres_conn_id='airflow-postgresql') 
    conn = postgres.get_conn() 
    cursor = conn.cursor()

    cursor.execute("COPY btc_price(datetime, open,high,low,close) FROM '/tmp/price_df.csv' DELIMITER ','CSV HEADER;")

    
    #cursor.execute("CREATE TABLE IF NOT EXISTS btc_price (id SERIAL PRIMARY KEY, Datetime DATE UNIQUE NOT NULL, Open FLOAT NOT NULL, High FLOAT NOT NULL, Low FLOAT NOT NULL, Close FLOAT NOT NULL);")
    conn.commit()






with DAG(
    'BTC_Prices',
    default_args=default_args,
    description='Getting the BTC price from Yahoo',
    #schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 4, 14, tz="UTC"),
) as dag:

    # task1 ==> extract data
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        dag=dag,
    )

    # task2 ==> transform data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        dag=dag,
    )
  

    # task3 ==> load data
    load_data = PythonOperator(
        task_id='load_data',
        provide_context=True,
        python_callable=load,
        dag=dag,
    )
       
    
   



#Order of tasks 
extract_data >> transform_data >> load_data