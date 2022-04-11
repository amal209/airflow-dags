from asyncio import Task
from airflow import DAG
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator
import yfinance 


default_args = {
    'retries':2
}

with DAG(
    'BTC Price',
    default_args=default_args,
    description='Getting the BTC price from Yahoo',
    #schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 4, 11, tz="UTC"),
) as dag:
    run_etl = PythonOperator(
        task_id='extract_data',
        python_callable=extractData,
        dag=dag,
    )

def extractData():
    #calling Yahoo finance API and requesting to get data for the last 22 hours, with an interval of 15 minutes.
    data = yfinance.download(tickers='BTC-USD', period = '22h', interval = '15m')
    data
    print('Data extracted')

run_etl








#calling Yahoo finance API and requesting to get data for the last 22 hours, with an interval of 15 minutes.
data = yf.download(tickers='BTC-USD', period = '22h', interval = '15m')
data