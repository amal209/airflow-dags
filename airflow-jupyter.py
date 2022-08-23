from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


default_args = {
    'retries':2
}



with DAG(
    dag_id='example2_papermill_operator',
    default_args=default_args,
    start_date=datetime(2022, 8, 23),
    template_searchpath='/opt/scripts',
    #template_searchpath='/dags_airflow-dags',
) as dag:
    notebook_task = PapermillOperator(
        task_id="run_example_notebook",
        input_nb="notebook.ipynb",
        output_nb="/opt/scripts/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

#order 
notebook_task
