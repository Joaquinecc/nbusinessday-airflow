from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from b_day import NBusinessDay
from pendulum import Time


def print_hello():
    print('Hello Wolrd')

with DAG(
    'hellow_world',
    start_date=datetime(2022,1,1),
    timetable=NBusinessDay(20,Time(16,25 )),
    catchup=False
    ) as dag:
    hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)
hello_operator
