import requests
import pendulum
import json

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

URL_API = 'https://official-joke-api.appspot.com/random_joke'

def fetch_joke(**context):
    response = requests.get(URL_API)
    if response.status_code == 200:
        joke = response.json()
        return joke
    else:
        raise Exception(f"Failed to fetch joke: {response.status_code} {response.text}")

def save_joke_to_file(ti, **context):
    joke = ti.xcom_pull(task_ids='fetch_joke_task')
    print(f"Received joke: {joke}")
    joke_text = f"{joke['setup']} - {joke['punchline']}"
    with open('/tmp/joke.txt', 'w') as file:
        file.write(joke_text)
    print(f"Joke saved to /tmp/joke.txt: {joke_text}")

with DAG(
    dag_id='jocking_api_dag',
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz='UTC'),
    catchup=False,
):
    fetch_joke_task = PythonOperator(
        task_id='fetch_joke_task',
        python_callable=fetch_joke,
        do_xcom_push=True,
    )
    save_joke_task = PythonOperator(
    task_id='save_joke_task',
    python_callable=save_joke_to_file,
    )
    
    fetch_joke_task >> save_joke_task
