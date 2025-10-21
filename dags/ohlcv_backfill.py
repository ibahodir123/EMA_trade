"""
Airflow DAG для исторической загрузки OHLCV.

Запускается on-demand или по расписанию, чтобы догрузить историю.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "uptrend-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@uptrend.local"],
    "retries": 1,
}


def run_backfill(**context) -> None:
    """
    Точка входа для вызова ingestion.historical_loader.

    TODO:
        - подтягивать список символов/таймфреймов из хранилища конфигураций;
        - инициировать запуск асинхронного скрипта через asyncio runner;
        - публиковать статус в Slack/Monitoring.
    """
    raise NotImplementedError("Implement backfill execution logic")


with DAG(
    "ohlcv_backfill",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["ohlcv", "ingestion"],
) as dag:
    backfill_task = PythonOperator(
        task_id="run_backfill",
        python_callable=run_backfill,
        provide_context=True,
    )
