if [[ ! -e /opt/airflow/dags/file/random_number.txt ]]; then
    mkdir -p "$AIRFLOW_HOME/dags/file"
    touch "$AIRFLOW_HOME/dags/file/random_number.txt"
fi