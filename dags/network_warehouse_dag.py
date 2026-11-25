from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "pid-team-c",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="network_warehouse_etl",
    default_args=default_args,
    description="ETL 7 hari Jaringan Kampus, data quality, dan warehouse",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["pid", "network", "warehouse"],
) as dag:
    run_etl = BashOperator(
        task_id="extract_clean_load",
        bash_command="python /opt/airflow/etl/etl_network.py",
        env={
            "POSTGRES_DSN": "postgresql://pid:pidpassword@postgres:5432/network_warehouse",
            "MONGODB_URI": "mongodb://pid:pidpassword@mongodb:27017/network_telemetry?authSource=admin",
        },
        append_env=True,
    )
