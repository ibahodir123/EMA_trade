"""
Airflow DAG для инкрементальной загрузки OHLCV.

Периодически запускает обновление последних свечей и проверяет наличие пропусков.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "uptrend-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["alerts@uptrend.local"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def run_incremental(**context) -> None:
    """
    Выполняет догрузку последних свечей и проверку gap-check.

    TODO:
        - реализовать вызов stream_listener fallback;
        - обновлять проверочные отчёты и алерты;
        - вызывать gap-check с отправкой отчёта в Slack/Email.
    """
    raise NotImplementedError("Implement incremental ingestion")


with DAG(
    "ohlcv_incremental",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ohlcv", "ingestion"],
) as dag:
    incremental_task = PythonOperator(
        task_id="run_incremental",
        python_callable=run_incremental,
        provide_context=True,
    )
