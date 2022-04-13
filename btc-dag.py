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
import psycopg2.extras as extras

default_args = {
    'retries':2
}

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



with DAG(
    'BTC_Price',
    default_args=default_args,
    description='Getting the BTC price from Yahoo',
    #schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 4, 12, tz="UTC"),
) as dag:
    # task1 ===>  Extract data
    extract_data = PythonOperator(
        task_id='extract_data',
        #postgres_conn_id='airflow-postgresql',
        python_callable=extractLoadData,
        dag=dag,
    )
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
    )

    # task3 ===>  create table
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id='airflow-postgresql',
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )




#Order of tasks 
create_table >> extract_data 