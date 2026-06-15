# dags/retail_pipeline_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "dataops",
    "retries": 2,
}

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    description="Pipeline E-commerce: bronze -> silver -> gold -> ML",
    schedule_interval="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="cd /app && python ingestion_bronze.py",
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="cd /app && python transform_silver.py",
    )

    aggregate_gold = BashOperator(
        task_id="aggregate_gold",
        bash_command="cd /app && python aggregate_gold.py",
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command="cd /app && python train_model.py",
    )

    score_model = BashOperator(
        task_id="score_model",
        bash_command="cd /app && python score_model.py",
    )

    ingest_raw >> transform_silver >> aggregate_gold >> train_model >> score_model
