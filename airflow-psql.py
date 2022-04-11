from asyncio import Task
import datetime
from airflow import DAG
import pendulum
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

default_args = {
    'retries':2
}


with DAG(
    'connection_to_Postgresql',
    default_args=default_args,
    description='A simple tutorial DAG',
    #schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 4, 8, tz="UTC"),
    tags=['example'],
) as dag:

    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id='airflow-postgresql',
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
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
    get_all_pets = PostgresOperator(task_id="get_all_pets", postgres_conn_id='airflow-postgresql',sql="SELECT * FROM pet;")
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id='airflow-postgresql',
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        runtime_parameters={'statement_timeout': '3000ms'},
    )

#Order of tasks
    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date

