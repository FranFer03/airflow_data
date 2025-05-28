import json
import requests
import pandas as pd
import boto3
import pendulum


from airflow.operators.python import PythonOperator
from airflow.models.dag import DAG

OUTPUT_FN = "/tmp/bitcoin_dataset.csv" 
DATA_BUCKET_NAME = "binance-data"

def extract():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    reponse = requests.get(url)
    if reponse.status_code == 200:
        data = reponse.json()
        df = pd.DataFrame([data])
        return df.to_dict('records')
    else:
        raise Exception(f"Error fetching data: {reponse.status_code} - {reponse.text}")

def transform(ti):
    data = ti.xcom_pull(task_ids="extract")  # This is a list of dicts
    df = pd.DataFrame(data)                  # Convert directly to DataFrame
    df['price'] = pd.to_numeric(df['price'])
    df['timestamp'] = pd.to_datetime('now').isoformat()  # Convert to string
    return df.to_dict('records')

def load_to_local_storage(ti):
    bitcoin_data = ti.xcom_pull(task_ids=["transform"])[0]
    df = pd.DataFrame(data=bitcoin_data)
    df.to_csv(OUTPUT_FN, index=False)

def upload_to_minio():
    s3 = boto3.client(
        "s3",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        endpoint_url="http://host.docker.internal:9000"
    )
    bucket_name = DATA_BUCKET_NAME
    filepath = OUTPUT_FN

    # Usar timestamp en el nombre del archivo
    timestamp = pendulum.now().to_iso8601_string().replace(":", "-")
    key = f"bitcoin_{timestamp}.csv"

    # Crear bucket si no existe
    existing = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if bucket_name not in existing:
        s3.create_bucket(Bucket=bucket_name)

    # Subir archivo con nombre único
    s3.upload_file(filepath, bucket_name, key)
    print(f"Archivo {key} subido a bucket {bucket_name}")



with DAG(
    dag_id="bitcoin_binance",
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    schedule='*/3 * * * *',
    catchup=False,
    tags=['bitcoin', 'binance']
):

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    load_to_local = PythonOperator(
        task_id='load_to_local_storage',
        python_callable=load_to_local_storage,
    )
    upload_to_minio_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )
    extract >> transform_task >> load_to_local >> upload_to_minio_task


