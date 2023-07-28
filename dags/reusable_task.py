from airflow.decorators import dag, task
from datetime import datetime

# Création d'une tache reutilisable
@task
def addition(x,y):
    print(f'addition de x={x} et de y={y}')
    return x+y

@dag(
    dag_id = "reusable_task",
    start_date = datetime(2023,7,12),
    )
def reusable_dag():
    # Appel d'une tache
    debut = addition.override(task_id="debut")(1,2)
    # réutilisation multiple
    for i in range(10):
        debut >> addition.override(task_id=f"suite_{i}")(debut,i)

# appel du DAG
reusable_dag()