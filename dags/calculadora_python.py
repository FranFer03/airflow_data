import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def add(**context) -> int:
    params = context['params']
    a = params['a']
    b = params['b']
    result = a + b
    print(f"Suma: {a} + {b} = {result}")
    return result

def subtract(**context) -> int:
    params = context['params']
    a = params['a']
    b = params['b']
    result = a - b
    print(f"Resta: {a} - {b} = {result}")
    return result

def multiply(**context) -> int:
    params = context['params']
    a = params['a']
    b = params['b']
    result = a * b
    print(f"Multiplicación: {a} * {b} = {result}")
    return result

def divide(**context) -> float:
    params = context['params']
    a = params['a']
    b = params['b']
    if b == 0:
        raise ValueError("Cannot divide by zero")
    result = a / b
    print(f"División: {a} / {b} = {result}")
    return result

with DAG(
    dag_id='calculadora_dag',
    schedule=None,
    start_date=pendulum.datetime(2022, 1, 1, tz='UTC'),
    catchup=False,
    params={
        'a': 10,
        'b': 5
    }
) as dag:
    
    add_task = PythonOperator(
        task_id='add_task',
        python_callable=add,
        do_xcom_push=True,
    )
    
    subtract_task = PythonOperator(
        task_id='subtract_task',
        python_callable=subtract,
        do_xcom_push=True
    )
    
    multiply_task = PythonOperator(
        task_id='multiply_task',
        python_callable=multiply,
        do_xcom_push=True
    )
    
    divide_task = PythonOperator(
        task_id='divide_task',
        python_callable=divide,
        do_xcom_push=True
    )
    
    add_task >> subtract_task >> multiply_task >> divide_task