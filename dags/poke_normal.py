import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task


@dag(
   schedule=None,
   start_date=pendulum.datetime(2022, 1, 1, tz='UTC'),
   catchup=False
)
def pokemonsflow_dag():

    @task
    def extract() -> list:
        url = 'http://pokeapi.co/api/v2/pokemon'
        params = {
            'limit': 20
        }
        response = requests.get(url=url, params=params)
        json_response = response.json()
        results = json_response['results']
        pokemons = [requests.get(url=result['url']).json() for result in results]

        return pokemons

    @task
    def transform(pokemons: list) -> list:
        columns = [
           'name',
           'order',
           'base_experience',
           'height',
           'weight'
        ]
        df = pd.DataFrame(data=pokemons, columns=columns)
        df = df.sort_values(['base_experience'], ascending=False)
        pokemons = df.to_dict('records')

        return pokemons

    @task
    def load(pokemons: list):
        df = pd.DataFrame(data=pokemons)
        df.to_csv('pokemons_dataset.csv', index=False)

    # ETL pipeline

    # extract
    extracted_pokemons = extract()

    # transform
    transformed_pokemons = transform(pokemons=extracted_pokemons)

    # load
    load(pokemons=transformed_pokemons)


dag = pokemonsflow_dag()