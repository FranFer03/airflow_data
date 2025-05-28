import pandas as pd
import pendulum
import requests
import json
import boto3
import os

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLValueCheckOperator
from airflow.utils.task_group import TaskGroup

POSTGRES_CONN_ID = "postgres_default"
DATA_BUCKET_NAME = "pokemon-data"
OUTPUT_FN = "/tmp/pokemons_dataset.csv"  # Cambiado a /tmp (escribible)

def transform(ti) -> list:
    pokemons = ti.xcom_pull(task_ids=["extract"])[0]
    pokemons = json.loads(pokemons)['results']
    columns = ['name', 'order', 'base_experience', 'height', 'weight']
    df = pd.DataFrame(data=pokemons, columns=columns)
    df = df.reset_index(names='idx')
    df['idx'] = df['idx'] + 1

    df['order'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('order'))
    df['base_experience'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('base_experience'))
    df['height'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('height'))
    df['weight'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('weight'))

    df = df.sort_values(['base_experience'], ascending=False)
    return df.to_dict('records')


def load_to_local_storage(ti):
    pokemons = ti.xcom_pull(task_ids=["transform"])[0]
    df = pd.DataFrame(data=pokemons)
    df.to_csv(OUTPUT_FN, index=False)


def load_to_remote_postgres(ti):
    pokemons = ti.xcom_pull(task_ids=["transform"])[0]
    df = pd.DataFrame(data=pokemons)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df.to_sql(name='pokemondata', con=pg_hook.get_sqlalchemy_engine(), if_exists='replace')


def upload_to_minio():
    s3 = boto3.client(
        "s3",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        endpoint_url="http://host.docker.internal:9000"
    )
    bucket_name = DATA_BUCKET_NAME
    key = "pokemons_dataset.csv"
    filepath = OUTPUT_FN

    # Crear bucket si no existe
    existing = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if bucket_name not in existing:
        s3.create_bucket(Bucket=bucket_name)

    # Subir archivo
    s3.upload_file(filepath, bucket_name, key)
    print(f"Archivo {key} subido a bucket {bucket_name}")


with DAG(
    dag_id="pokemon_http_op_dag3",
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz='UTC'),
    catchup=False
):

    extract = HttpOperator(
        task_id="extract",
        http_conn_id="pokemon_api_conn",
        endpoint="/pokemon",
        method="GET",
        log_response=True,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_to_local = PythonOperator(
        task_id='load_to_local_storage',
        python_callable=load_to_local_storage,
    )

    load_to_postgres = PythonOperator(
        task_id='load_to_remote_postgres',
        python_callable=load_to_remote_postgres,
    )

    upload_file = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )

    with TaskGroup(group_id="row_quality_checks") as quality_check_group:
        sql_check_count = SQLExecuteQueryOperator(
            task_id="sql_check_count_task",
            conn_id=POSTGRES_CONN_ID,
            sql="sql/check_count.sql",
            show_return_value_in_logs=True,
        )

        sql_create_table = SQLExecuteQueryOperator(
            task_id="sql_create_table_task",
            conn_id=POSTGRES_CONN_ID,
            sql="sql/create_table_from_other.sql",
            show_return_value_in_logs=True,
        )

        value_check = SQLValueCheckOperator(
            task_id="check_row_count",
            conn_id=POSTGRES_CONN_ID,
            sql="sql/check_count.sql",
            pass_value=20,
        )

    extract >> transform_task >> [load_to_local, load_to_postgres] >> upload_file
    [load_to_local, load_to_postgres] >> quality_check_group
