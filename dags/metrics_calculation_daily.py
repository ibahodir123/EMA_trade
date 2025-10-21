"""
Airflow DAG для ночного расчёта метрик.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from metrics.config_loader import load_metric_params
from metrics.jobs.calculation_job import JobConfig, MetricsCalculationJob
from metrics.jobs.clickhouse_client import ClickHouseConfig
from metrics.jobs.parquet_reader import RawStoreConfig
from metrics.jobs.secrets import load_clickhouse_secret

DEFAULT_ARGS = {
    "owner": "metrics-engine",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@uptrend.local"],
    "retries": 1,
}


def _parse_list(env_name: str, default: str) -> list[str]:
    value = os.environ.get(env_name, default)
    return [item.strip() for item in value.split(",") if item.strip()]


def run_daily_metrics(**context) -> None:
    data_start = context["data_interval_start"].int_timestamp
    data_end = context["data_interval_end"].int_timestamp

    metric_params = load_metric_params()
    secret = load_clickhouse_secret()

    job_cfg = JobConfig(
        exchanges=_parse_list("METRICS_EXCHANGES", "BINANCE"),
        symbols=_parse_list("METRICS_SYMBOLS", "BTCUSDT,ETHUSDT"),
        timeframes=_parse_list("METRICS_TIMEFRAMES", "5m,1h,4h,1d"),
        raw_store=RawStoreConfig(root_path=os.environ.get("RAW_STORE_PATH", "s3://uptrend-raw-store")),
        clickhouse=ClickHouseConfig(
            host=secret.host,
            port=secret.port,
            username=secret.username,
            password=secret.password,
            secure=bool(int(os.environ.get("CLICKHOUSE_SECURE", "1"))),
            database=os.environ.get("CLICKHOUSE_DATABASE", "uptrend"),
        ),
    )

    job = MetricsCalculationJob(job_cfg, metric_params)
    job.writer.health_check()
    job.run_full(start_ts=data_start, end_ts=data_end)


with DAG(
    dag_id="metrics_calculation_daily",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 5 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["metrics", "calculation"],
) as dag:
    PythonOperator(
        task_id="run_daily_metrics",
        python_callable=run_daily_metrics,
        provide_context=True,
    )
