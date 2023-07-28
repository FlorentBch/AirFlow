# AirFlow démo

## Téléchargment du docker-compose

```bash
wget -O docker-compose.yaml https://airflow.apache.org/docs/apache-airflow/2.6.2/docker-compose.yaml
```

## Configuration du doker-compose

Ajout d'un adminer afin de suivre la bdd postgres
```yaml
  adminer:
    image: adminer
    restart: always
    ports:
      - 7080:8080
    depends_on:
      - postgres
```

## Installation du docker-compose

Permet de lancer l'installation du docker-compose en dependant du fichier common qui lui même crée des conteneur

```bash
docker-compose up airflow.init
docker-compose up
```

## Scripting objet DAG

Un fichier DAG est un fichier de configuration pour paramettrer des flux de travail

### Déclaration par le contexte

```python
import datetime
# import des librairies airflow
from airflow import DAG
from airflow.operators.empty import EmptyOperator
with DAG(
  dag_id="my_dag_name",
  start_date = datetime.datetime(2021, 1, 1),
  schedule="@daily",
  default_args={"key":"value"}
):
  # contexte d'execution de mes taches
  EmptyOperator(task_id="task")
```