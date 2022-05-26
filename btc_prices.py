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
#import sql  # the patched version (file is named sql.py)
from sqlalchemy import create_engine


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
    #rename df columns ( so we can easly access to them using grafana)
    price_df.rename(columns = {'Datetime':'datetime', 'Open':'open', 'High':'high', 'Low':'low', 'Close':'close'}, inplace = True)
    print("PRICE DATAFRAME : ",price_df)
    price_df.to_csv("/tmp/price_df.csv", index=False)


# load data
def load():
    #get data
    price_df = pd.read_csv("/tmp/price_df.csv")    
    # load to postgres
    engine = create_engine('postgresql://postgres:echoabJIbg3d53@10.102.86.9:5432/airflow_db')
    price_df.to_sql('btc_price', engine)

with DAG(
    'BTC_Prices_ETL',
    default_args=default_args,
    description='Getting the BTC price from Yahoo',
    #schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 5, 26, tz="UTC"),
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