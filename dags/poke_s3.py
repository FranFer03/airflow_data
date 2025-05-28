import pandas as pd
import pendulum
import requests
import boto3
import os

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
)
def pokemonsflow_dag2():

    @task
    def extract() -> list:
        url = "http://pokeapi.co/api/v2/pokemon"
        response = requests.get(url, params={"limit": 20})
        results = response.json()["results"]
        pokemons = [requests.get(url=r["url"]).json() for r in results]
        return pokemons

    @task
    def transform(pokemons: list) -> list:
        df = pd.DataFrame(pokemons)[["name", "order", "base_experience", "height", "weight"]]
        df = df.sort_values("base_experience", ascending=False)
        return df.to_dict("records")

    @task
    def load(pokemons: list) -> str:
        df = pd.DataFrame(pokemons)
        output_path = "/tmp/pokemons_dataset.csv"  # Carpeta con permisos de escritura
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def upload_to_minio(filepath: str):
        s3 = boto3.client(
            "s3",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            endpoint_url="http://host.docker.internal:9000",  # <--- este es el correcto
        )
        bucket_name = "pokemon-data"
        s3.upload_file(filepath, bucket_name, "pokemons_dataset.csv")
        print("Archivo subido a MinIO correctamente.")


    # Orquestación
    raw = extract()
    transformed = transform(raw)
    csv_path = load(transformed)
    upload_to_minio(csv_path)


dag = pokemonsflow_dag2()
