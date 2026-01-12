from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="olist_spark_pipeline",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "olist", "etl"],
) as dag:

    extract = BashOperator(
        task_id="extract_data",
        bash_command="python3 /opt/airflow/src/extract.py",
    )

    cleaning = BashOperator(
        task_id="clean_data",
        bash_command="python3 /opt/airflow/src/cleaning.py",
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command="python3 /opt/airflow/src/transform.py",
    )

    load = BashOperator(
        task_id="load_to_dwh",
        bash_command="python3 /opt/airflow/src/load_to_dwh.py",
    )

    extract >> cleaning >> transform >> load
