from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='example_papermill_operator',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2022, 8, 11),
    #template_searchpath='/usr/local/airflow/include',
    template_searchpath='/dags_airflow-dags',
    catchup=False
) as dag_1:

    notebook_task = PapermillOperator(
        task_id="run_example_notebook",
        input_nb="dags_airflow-dags/test.ipynb",
        output_nb="dags_airflow-dags/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )