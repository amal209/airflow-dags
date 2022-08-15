from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


default_args = {
    'retries':2
}



with DAG(
    dag_id='example_papermill_operator',
    default_args=default_args,
    start_date=datetime(2022, 8, 15),
    #template_searchpath='/usr/local/airflow/include',
    #template_searchpath='/dags_airflow-dags',
) as dag:
    notebook_task = PapermillOperator(
        task_id="run_example_notebook",
        input_nb="/dags_airflow-dags/test.ipynb",
        output_nb="/dags_airflow-dags/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

#order 
notebook_task
