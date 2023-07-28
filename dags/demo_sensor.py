from airflow import DAG
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.python import PythonOperator


# Méthode pour vérifier si un sensor à echoué
def _failure_callback(context):
    """
    The function `_failure_callback` is a callback function that is triggered when a sensor times out in
    Airflow.
    
    :param context: The `context` parameter is a dictionary that contains information about the task
    execution context. It typically includes information such as the task instance, the execution date,
    and any relevant configuration parameters
    """
    if isinstance(context['exception'], AirflowSensorTimeout ):
        print(context)
        print("sensor timeout")
    

# Fausse fonction
def _store():
    pass

def _process():
    pass

with DAG(
    dag_id="demo_sensor",
    schedule_interval="@daily",
    catchup=False,
    start_date=datetime(2023,7,12)
) as dag:
    # Taches multiples
    check_parteners = [ FileSensor(
        task_id = f'sensor_{partner}',
        poke_interval = 120,
        timeout = 60*30,
        mode = "poke",
        on_failure_callback = _failure_callback,
        filepath = f'partner_{partner}.txt',
        fs_conn_id='partner_data_path'
    ) for partner in ["Jean", "Karene", "Jacqueline"]]
    
    # Tache 2
    process_data = PythonOperator(
        task_id = "process_data",
        python_callable = _process
    )
    
    # Tache 3
    stockage_data = PythonOperator(
        task_id = "stockage_data",
        python_callable = _store
    )
    
    # Appel de mes taches
    check_parteners >> process_data >> stockage_data