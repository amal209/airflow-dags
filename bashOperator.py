from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 


default_args = {
    'retries':2
}
spark_master = ("spark://"
                "spark-master-svc"
                ":7077")
                
command = ("spark-submit "
            "--master {master} "
            "/tmp/test.py").format(master=spark_master)

with DAG(
    dag_id='bash_operator',
    default_args=default_args,
    start_date=datetime(2022, 9, 23),
) as dag:
    t2 = BashOperator(task_id='test_bash_operator',bash_command=command, dag=dag)

t2
