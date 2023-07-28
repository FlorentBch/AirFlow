from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
import requests
import json
import pandas as pd
import psycopg2 as pg
import os


# definition de mon dag
@dag(
    dag_id = "import_data",
    schedule_interval="@once",
    start_date = datetime(2023,7,11),
    catchup = False,
    dagrun_timeout = timedelta(minutes=10),
    
)
def extract_to_postgres():
    # Tache 1 Create table if not exist
    create_drivers_table = PostgresOperator(
        task_id="create_drivers_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="sql/drivers_table.sql"
    )
    
    # Tache 2 recuperation des data via une API
    @task(task_id="get_data_to_local")
    def get_data_to_local():
        # URL de ma requete API
        url = "https://data.cityofnewyork.us/resource/4tqt-y424.json"
        response = requests.get(url)
        
        # Récuperation des données du "content" en json
        data_json = json.loads(response.content)
        # Utilisation de pandas pour charger mes datas en CSV
        df = pd.DataFrame(data_json)
        df.to_csv("/opt/airflow/dags/file/drivers.csv",
                  sep=";",
                  escapechar="\\",
                  encoding='utf-8',
                  quoting=1
        )
        
    # Tache 3 insertion des données dans ma base de données
    @task(task_id="load_to_postgres")
    def load_to_postgres():
        try:
            dbconnect = pg.connect(
            "dbname='airflow' user='airflow' password='airflow' host='postgres'"
            )
            # Création d'un cursor pour intéragir avec la bdd
            cursor = dbconnect.cursor()
            with open('/opt/airflow/dags/file/drivers.csv', 'r') as source:
                # skip la ligne de headers
                next(source)
                for row in source:
                    row_split = row.split(";")
                    cursor.execute("""
                        INSERT INTO drivers_data
                        VALUES ('{}','{}','{}','{}','{}')
                        """.format(
                            row_split[1],
                            row_split[2],
                            row_split[3],
                            row_split[4],
                            row_split[5]
                        )
                    )
            dbconnect.commit()
        except Exception as error:
            print(error)
        finally:
            dbconnect.close()
            
        # Tache 4 Supprimer le fichier temporaire
    @task(task_id="delete_file")
    def delete_file():
        os.remove("/opt/airflow/dags/file/drivers.csv")

    #  Relation entre mes taches
    create_drivers_table >> get_data_to_local() >> load_to_postgres() >> delete_file()

extract_to_postgres()