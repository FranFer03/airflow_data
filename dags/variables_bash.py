import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='variable_bash_dag',
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=pendulum.duration(minutes=60),
    params={'first_var': 'valor_de_mi_variable'}  # Define la variable en params
) as dag:

    ejecutar_comando = BashOperator(
        task_id='ejecutar_comando',
        bash_command="echo 'Hola soy bash, mi variable es: {{ params.first_var }}'",  # Accede a la variable con params.
        do_xcom_push=False,
    )
    ejecutar_comando