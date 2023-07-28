from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Definition d'un dag
@dag(
    dag_id="demo_branch_simple",
    start_date=datetime(2023,7,12),
    catchup=False
)
def simpleBranch():
    # création de taches multiple
    t0 = EmptyOperator(task_id="t0")
    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")
    t3 = EmptyOperator(task_id="t3")
    t4 = EmptyOperator(task_id="t4")
    t5 = EmptyOperator(task_id="t5")
    t6 = EmptyOperator(task_id="t6")
    
    # Relation entre mes taches
    t0 >> t1
    t1 << t2
    t1 >> [t3, t4]

# appel de mon DAG
simpleBranch()
    