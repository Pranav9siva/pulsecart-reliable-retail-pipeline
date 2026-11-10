from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pulsecart_retail_batch",
    description="Extract, transform, validate, and report UCI Online Retail II data",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "pyspark", "retail"],
) as dag:
    extract_source = BashOperator(
        task_id="extract_source",
        bash_command="python -m pulsecart.cli extract",
        cwd="/opt/pulsecart",
        append_env=True,
    )
    transform = BashOperator(
        task_id="transform_bronze_silver_gold",
        bash_command="python -m pulsecart.cli batch",
        cwd="/opt/pulsecart",
        append_env=True,
    )
    quality = BashOperator(
        task_id="data_quality_and_reconciliation",
        bash_command="python -m pulsecart.cli quality",
        cwd="/opt/pulsecart",
        append_env=True,
    )
    reports = BashOperator(
        task_id="publish_analytics_reports",
        bash_command="python -m pulsecart.cli report",
        cwd="/opt/pulsecart",
        append_env=True,
    )

    extract_source >> transform >> quality >> reports

