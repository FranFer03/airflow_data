import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='time_bash_dag',
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=pendulum.duration(minutes=60)
) as dag:

    ejecutar_comando = BashOperator(
        task_id='ejecutar_comando',
        bash_command="echo 'Hola soy bash, la fecha de hoy es: {{ ds }}'",
        do_xcom_push=False,
    )
    ejecutar_comando