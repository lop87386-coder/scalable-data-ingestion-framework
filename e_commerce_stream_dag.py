"""
DAG Airflow - E-commerce_stream (client_e-commerce)
Généré par le moteur local Innovation. Orchestration Bronze -> Silver -> Gold.
# Pipeline 100% batch : Bronze -> Silver -> Gold orchestrés dans l'ordre.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SCRIPT_PATH = os.getenv("PIPELINE_SCRIPT_PATH", "/opt/pipelines/client_e_commerce_e_commerce_stream_pipeline.py")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="client_e_commerce_e_commerce_stream_pipeline",
    description="Pipeline jdbc pour E-commerce_stream (client_e-commerce)",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["client_e_commerce", "e_commerce_stream", "jdbc"],
) as dag:

    bronze_task = BashOperator(
        task_id="bronze",
        bash_command=f"python {SCRIPT_PATH} bronze",
    )

    silver_task = BashOperator(
        task_id="silver",
        bash_command=f"python {SCRIPT_PATH} silver",
    )

    gold_task = BashOperator(
        task_id="gold",
        bash_command=f"python {SCRIPT_PATH} gold",
    )

    bronze_task >> silver_task >> gold_task
