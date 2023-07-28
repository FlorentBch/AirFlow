# créer une tache DAG qui se lance tous les jours, pour créer un nombre aléatoire, et enregister ça à la date du jour dans un fichier texte

# tous les jours a partir du 1er juillet 2023

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import random
import os

@dag(
    dag_id = "Exercice001",
    start_date=datetime(2023, 7, 1),
    schedule_interval="@daily",
    catchup=True,
)
def mon_dag():
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    def generate_random_number():
        return random.randint(0, 10000000)
    
    def write_random_number():
        path = os.path.join(os.path.dirname(__file__), "random_number.txt") # __file__ = dags\exo1.py ; os.path.dirname(__file__) = dags
        with open(path, "a") as f:
            f.write(str(generate_random_number())+" "+str(datetime.now())+"\n")

    t2 = PythonOperator(
        task_id="generate_random_number",
        python_callable=generate_random_number,
    )
    
    t3 = PythonOperator(
        task_id="write_random_number",
        python_callable=write_random_number,
    )

    t1 >> t2 >> t3

mon_dag()