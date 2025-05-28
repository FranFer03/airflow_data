import pandas as pd
import pendulum
import requests
import json

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator


def transform(ti) -> list:
    pokemons = ti.xcom_pull(task_ids=["extract"])[0]

    pokemons = json.loads(pokemons)['results']
    columns = [
       'name',
       'order',
       'base_experience',
       'height',
       'weight'
    ]
    df = pd.DataFrame(data=pokemons, columns=columns)
    df = df.reset_index(names='idx')
    df['idx'] = df['idx'] + 1

    df['order'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('order'))
    df['base_experience'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('base_experience'))
    df['height'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('height'))
    df['weight'] = df['idx'].apply(lambda x: requests.get(url=f"https://pokeapi.co/api/v2/pokemon/{x}/").json().get('weight'))
    df = df.sort_values(['base_experience'], ascending=False)
    pokemons = df.to_dict('records')

    return pokemons


def load(ti):
    pokemons = ti.xcom_pull(task_ids=["transform"])[0]
    df = pd.DataFrame(data=pokemons)
    df.to_csv('pokemons_dataset.csv', index=False)


with DAG(
   dag_id="pokemon_http_op_dag",
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

    transform = PythonOperator(
       task_id='transform',
       python_callable=transform,
    )

    load = PythonOperator(
       task_id='load',
       python_callable=load,
    )

    extract >> transform >> load