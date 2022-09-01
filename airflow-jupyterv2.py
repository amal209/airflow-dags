from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 


default_args = {
    'retries':2
}

with DAG(
    dag_id='spark_submit_operator',
    default_args=default_args,
    start_date=datetime(2022, 9, 1),
) as dag:
    spark_submit_local = SparkSubmitOperator(
		application ='/tmp/test.py' ,
		conn_id= 'spark_default', 
		task_id='spark_submit_task', 
		dag=dag
		)

spark_submit_local
    
