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

#load data
def execute_query_with_conn_obj():#(query): 
    hook = PostgresHook(postgres_conn_id='airflow-postgresql') 
    conn = hook.get_conn() 
    cur = conn.cursor()
    sql_query = """
        CREATE TABLE IF NOT EXISTS btc_price (
            Datetime DATE PRIMARY KEY,
            Open FLOAT NOT NULL,
            High FLOAT NOT NULL,
            Low FLOAT NOT NULL,
            Close FLOAT NOT NULL
    )
    """ 
    cur.execute(sql_query)
    print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")



    

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
        python_callable=extract,
        dag=dag,
    )

    # task2 ==> transform data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id='execute_query',
        provide_context=True,
        python_callable=execute_query_with_conn_obj,
        #op_kwargs={'query': 'CREATE TABLE IF NOT EXISTS btc_prices (Datetime DATE PRIMARY KEY, Open FLOAT NOT NULL, High FLOAT NOT NULL, Low FLOAT NOT NULL, Close FLOAT NOT NULL)'},
        dag=dag)
'''
    
    # task3 ===>  create table and load df to table
    load_data = PostgresOperator(
        task_id="load_data_to_postgres",
        postgres_conn_id='airflow-postgresql',
        
        
    )
'''





#Order of tasks 
extract_data >> transform_data >> t3