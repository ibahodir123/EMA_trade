"""Airflow DAG that orchestrates the Rule Engine stub."""

from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from metrics.jobs.airflow_integration import DEFAULT_CLICKHOUSE_CONN_ID
from rule_engine.processor import generate_trading_signals

DEFAULT_ARGS = {
    "owner": "rule-engine",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@uptrend.local"],
    "retries": 1,
}

CLICKHOUSE_CONN_ENV = "CLICKHOUSE_CONN_ID"
CONFIG_PATH_ENV = "RULE_ENGINE_CONFIG_PATH"
SYMBOLS_ENV = "RULE_ENGINE_SYMBOLS"
EXCHANGES_ENV = "RULE_ENGINE_EXCHANGES"


def _parse_list(env_name: str) -> list[str] | None:
    raw = os.environ.get(env_name)
    if not raw:
        return None
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or None


def run_rule_engine(**context) -> None:
    execution_dt = context["data_interval_end"]
    signals = generate_trading_signals(
        execution_dt=execution_dt,
        clickhouse_conn_id=os.environ.get(CLICKHOUSE_CONN_ENV, DEFAULT_CLICKHOUSE_CONN_ID),
        config_path=os.environ.get(CONFIG_PATH_ENV, "config/rule_engine_config.yaml"),
        exchanges=_parse_list(EXCHANGES_ENV),
        symbols=_parse_list(SYMBOLS_ENV),
    )

    context["ti"].xcom_push(
        key="signals",
        value=[asdict(signal) for signal in signals],
    )


with DAG(
    dag_id="trading_signals_daily",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["rule-engine", "signals"],
) as dag:
    wait_for_metrics = ExternalTaskSensor(
        task_id="wait_for_metrics_daily",
        external_dag_id="metrics_calculation_daily",
        external_task_id="run_daily_metrics",
        mode="reschedule",
        poke_interval=300,
        timeout=3600,
    )

    build_signals = PythonOperator(
        task_id="generate_trading_signals",
        python_callable=run_rule_engine,
        provide_context=True,
    )

    wait_for_metrics >> build_signals
