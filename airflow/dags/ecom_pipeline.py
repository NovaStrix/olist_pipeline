from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

def on_fail(context):
    print("FAILED")

def on_success(context):
    print("SUCCESS")


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_fail,
    "on_success_callback": on_success,
}


with DAG(
    dag_id="olist_spark_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["spark", "olist", "etl"],
) as dag:

    extract = SparkSubmitOperator(
        task_id="extract_data",
        execution_timeout=timedelta(minutes=20),
        application="/opt/airflow/src/extract.py",
        conn_id='spark_default',
        # verbose=True,
    )

    cleaning = SparkSubmitOperator(
        task_id="clean_data",
        execution_timeout=timedelta(minutes=20),
        application="/opt/airflow/src/cleaning.py",
        conn_id='spark_default',
        # verbose=True,
    )

    transform = SparkSubmitOperator(
        task_id="transform_data",
        execution_timeout=timedelta(minutes=20),
        application="/opt/airflow/src/transform.py",
        conn_id='spark_default',
        # verbose=True,
    )

    load = SparkSubmitOperator(
        task_id="load_to_dwh",
        execution_timeout=timedelta(minutes=40),
        application="/opt/airflow/src/load_to_dwh.py",d
        conn_id='spark_default',
        # verbose=True,
    )

    extract >> cleaning >> transform >> load
