from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag
from random import randint
import os
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def nb_random():
    return randint(0,10000)

def save_inFile(file, **context):
    with open(file,"a") as f:
        msg = "date : {}, nombre aléatoire : {}"\
            .format(context['execution_date'],nb_random())
        f.write(msg)
        f.write("\n")

@dag(
    dag_id="Correction_exercice_01",
    schedule="@daily",
    start_date=datetime(2023,7,1),
    catchup = True
)  
def mon_dag():
    create_file = BashOperator(
        task_id="create_file",
        bash_command="/opt/airflow/dags/file/mon_script.sh ",
    )
    
    save_data = PythonOperator(
        task_id="save_data",
        depends_on_past=False,
        python_callable=save_inFile,
        provide_context=True,
        op_kwargs={
            'file': '/opt/airflow/dags/file/random_number.txt',
        },
    )
    
    create_file >> save_data
mon_dag()