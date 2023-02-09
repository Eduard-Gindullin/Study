
from airflow.models import Variable
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print('Airflow')
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

conn_str = 'clickhouse://default:@localhost/default'

engine = create_engine(conn_str)
session = sessionmaker(bind=engine)()

with DAG (dag_id="first_dag", start_date=datetime(2023, 1, 23), schedule="0 0 * * *") as dag:
    bash_task = BashOperator(task_id="hell1", bash_command="echo hello")
    python_task = PythonOperator(task_id="worl1", python_callable = hello)
    parsing_1 = BashOperator(task_id="parsing_cleaning_creating", \
            bash_command="/opt/airflow/dags/lentaParcer.py", \
            do_xcom_push=False) 