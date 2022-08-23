from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator


default_args = {
    'retries':2
}
spark_master = ("spark://spark-master-0.spark-headless.spark.svc.cluster.local:7077")

command = ("spark-submit "
           "--master {master} "
           #"--py-files package1.zip "
           "/tmp/test.py"
           ).format(master=spark_master)

#dag = DAG(............,schedule_interval='@daily')#

with DAG(
    dag_id='example_bach_operator',
    default_args=default_args,
    start_date=datetime(2022, 8, 23),
    #schedule_interval='@daily',
) as dag:
    #t2 = BashOperator(task_id='test_bash_operator',bash_command=command, dag=dag)
    task = SSHOperator(task_id='ssh_spark_submit',dag=dag,command=command,ssh_conn_id='spark_master_ssh'
)

task
    
