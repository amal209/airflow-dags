from asyncio import Task
from datetime import date
from email.errors import CloseBoundaryNotFoundDefect

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
import yfinance 
import psycopg2
import numpy as np
import pandas as pd
import psycopg2.extras as extras


default_args = {
    'retries':2
}
'''
def extractLoadData():
    table="btc_price"
    #calling Yahoo finance API and requesting to get data for the last 22 hours, with an interval of 15 minutes.
    data = yfinance.download(tickers='BTC-USD', period = '22h', interval = '15m')
    data
    print('Data extracted')
    tpls = [tuple(x) for x in data.to_numpy()]
    
    # dataframe columns with Comma-separated
    cols = ','.join(list(data.columns))

    # SQL query to execute
    sql = """INSERT INTO %s(%s) VALUES(%%s,%%s,%%s,%%s,%%s)""" % (table, cols)

'''
def run_btc_price_etl():
    DATABASE_LOCATION = "postgres:abJIbg3d53@10.102.86.9/airflow_db"

    #calling Yahoo finance API and requesting to get data for the last 22 hours, with an interval of 15 minutes.
    data = yfinance.download(tickers='BTC-USD', period = '22h', interval = '15m')
    data
    print('Data extracted')

    price_df = pd.DataFrame(data , columns = ["Open", "High", "Low" , "Close"])
    print(price_df)
    


    # Load

    #engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = psycopg2.connect(
    host="10.102.86.9",
    database="airflow_db",
    user="postgres",
    password="abJIbg3d53")

    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS btc_price (
            Datetime DATE PRIMARY KEY,
            Open FLOAT NOT NULL,
            High FLOAT NOT NULL,
            Low FLOAT NOT NULL,
            Close FLOAT NOT NULL);
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        price_df.to_sql("btc_price", conn , if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")

with DAG(
    'BTC_Price',
    default_args=default_args,
    description='Getting the BTC price from Yahoo',
    #schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 4, 13, tz="UTC"),
) as dag:
    # task
    run_etl = PythonOperator(
        task_id='extract_data',
        #postgres_conn_id='airflow-postgresql',
        python_callable=run_btc_price_etl,
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
run_etl