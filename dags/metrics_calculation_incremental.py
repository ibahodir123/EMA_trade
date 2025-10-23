"""Airflow DAG for incremental metrics calculation."""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from metrics.config_loader import load_metric_params
from metrics.jobs.airflow_integration import (
    DEFAULT_CLICKHOUSE_CONN_ID,
    DEFAULT_S3_CONN_ID,
    build_clickhouse_config,
    build_raw_store_config,
)
from metrics.jobs.calculation_job import JobConfig, MetricsCalculationJob

DEFAULT_ARGS = {
    "owner": "metrics-engine",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@uptrend.local"],
    "retries": 3,
}

RAW_STORE_PATH_ENV = "RAW_STORE_PATH"
RAW_STORE_CONN_ENV = "RAW_STORE_CONN_ID"
CLICKHOUSE_CONN_ENV = "CLICKHOUSE_CONN_ID"
CLICKHOUSE_DB_ENV = "CLICKHOUSE_DATABASE"
CLICKHOUSE_SECURE_ENV = "CLICKHOUSE_SECURE"
LOOKBACK_ENV = "METRICS_INCREMENTAL_LOOKBACK"


def _parse_list(env_name: str, default: str) -> list[str]:
    value = os.environ.get(env_name, default)
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_scope() -> tuple[list[str], list[str], list[str], str]:
    exchanges = _parse_list("METRICS_EXCHANGES", "BINANCE")
    symbols = _parse_list("METRICS_SYMBOLS", "BTCUSDT,ETHUSDT")
    timeframes = _parse_list("METRICS_TIMEFRAMES", "5m,1h,4h,1d")
    raw_store_path = os.environ.get(RAW_STORE_PATH_ENV, "s3://uptrend-raw-store")
    return exchanges, symbols, timeframes, raw_store_path


def _build_configs(raw_store_path: str):
    raw_cfg = build_raw_store_config(
        conn_id=os.environ.get(RAW_STORE_CONN_ENV, DEFAULT_S3_CONN_ID),
        default_root_path=raw_store_path,
    )
    clickhouse_cfg = build_clickhouse_config(
        conn_id=os.environ.get(CLICKHOUSE_CONN_ENV, DEFAULT_CLICKHOUSE_CONN_ID),
        default_database=os.environ.get(CLICKHOUSE_DB_ENV),
        secure_default=bool(int(os.environ.get(CLICKHOUSE_SECURE_ENV, "0"))),
    )
    return raw_cfg, clickhouse_cfg


def run_incremental_metrics(**context) -> None:
    lookback_seconds = int(os.environ.get(LOOKBACK_ENV, "86400"))

    exchanges, symbols, timeframes, raw_store_path = _resolve_scope()
    raw_cfg, clickhouse_cfg = _build_configs(raw_store_path)
    metric_params = load_metric_params()

    job_cfg = JobConfig(
        exchanges=exchanges,
        symbols=symbols,
        timeframes=timeframes,
        raw_store=raw_cfg,
        clickhouse=clickhouse_cfg,
    )

    job = MetricsCalculationJob(job_cfg, metric_params)
    job.writer.health_check()
    job.run_incremental(lookback_seconds=lookback_seconds)


with DAG(
    dag_id="metrics_calculation_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["metrics", "calculation"],
) as dag:
    PythonOperator(
        task_id="run_incremental_metrics",
        python_callable=run_incremental_metrics,
        provide_context=True,
    )
