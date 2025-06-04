from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime, timedelta
import os
import time

class CustomFileSensor(BaseSensorOperator):
    def __init__(self, filepath, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
    
    def poke(self, context):
        self.log.info(f"Checking for file: {self.filepath}")
        exists = os.path.exists(self.filepath)
        if exists:
            self.log.info(f"✅ File found: {self.filepath}")
        else:
            self.log.info(f"⏳ File not found: {self.filepath}")
        return exists

with DAG(
    dag_id="file_sensor_python",
    start_date=datetime(2025, 4, 1),
    schedule=None,
    catchup=False,
) as dag:

    create_file = BashOperator(
        task_id="create_file",
        bash_command="echo 'test,data,$(date)' > /tmp/sensor_test.csv",
    )

    wait_for_file = CustomFileSensor(
        task_id="wait_for_file",
        filepath="/tmp/sensor_test.csv",
        poke_interval=10,
        timeout=120,
    )

    process_file = BashOperator(
        task_id="process_file",
        bash_command="echo 'Processing file:' && cat /tmp/sensor_test.csv",
    )

    create_file >> wait_for_file >> process_file