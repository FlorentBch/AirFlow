
"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
from __future__ import annotations

# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# [END import_module]
@task(task_id='t4')
def ma_tache_t4():
    for i in range(5):
        print("{{ds}}")
        print("{{ macros.ds_add(ds, 7)}}")

# [START instantiate_dag]
@dag(
    dag_id = "Exercice_00",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    # [END default_args]
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
) 
def dagg():
    # [END instantiate_dag]

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    # [START basic_task]
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
    )
    # [END basic_task]

    # [START jinja_template]
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t3 = PythonOperator(
        task_id="t4",
        depends_on_past=False,
        python_callable=ma_tache_t4,
    )
    # [END jinja_template]

    t1 >> [t2, t3]
# [END tutorial]
dagg()