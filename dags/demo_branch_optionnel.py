from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from random import randint

# Argument par defaut
default_args = {
    "start_date": datetime(2023,7,12),
    "retries": 1
}

# Ma fonction de choix multiple
def _mon_eval_model():
    accuracy = randint(0,100)
    print("accuracy : ", accuracy)
    if accuracy > 70:
        return ["super_accurate", "accurate"]
    elif accuracy > 50:
        return "accurate"
    elif accuracy <=50 :
        return "not_accurate"

# Déclaration de mon dag par contexte
with DAG(
    dag_id = "demo_branch_optionnel",
    schedule = "@once",
    catchup = False,
    default_args = default_args
) as dag:
    # Tache 1 simule la création du model
    t1 = DummyOperator(
        task_id ="create_model"
    )
    # tache 2 Tache de branchement sur son evaluation
    choose_best = BranchPythonOperator(
        task_id = "choose_best",
        python_callable = _mon_eval_model # L ll
    )
    # Tache du choix super précis
    super_accurate = DummyOperator(
        task_id ="super_accurate"
    )
    # Tache du choix précis
    accurate = DummyOperator(
        task_id ="accurate"
    )
    # Tache du choix Pas précis 
    not_accurate = DummyOperator(
        task_id ="not_accurate"
    )
    
    #  Relation entres mes taches
    t1 >> choose_best >> [super_accurate, accurate, not_accurate]