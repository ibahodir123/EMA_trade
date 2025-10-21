"""
ClickHouse client for Metrics Engine.

Обеспечивает bulk insert результатов расчёта в таблицы metrics_<tf>.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd
from clickhouse_connect import get_client


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int = 9440
    username: str = "default"
    password: str = ""
    secure: bool = True
    database: str = "uptrend"


class ClickHouseWriter:
    """Пакетная запись данных в ClickHouse."""

    def __init__(self, cfg: ClickHouseConfig) -> None:
        self.cfg = cfg
        self.client = get_client(
            host=cfg.host,
            port=cfg.port,
            username=cfg.username,
            password=cfg.password,
            secure=cfg.secure,
            database=cfg.database,
        )

    def insert_dataframe(self, table: str, df: pd.DataFrame) -> None:
        """Выполняет bulk insert DataFrame в таблицу ClickHouse."""
        if df.empty:
            return
        columns = list(df.columns)
        data = df.to_records(index=False).tolist()
        self.client.insert(table, data, column_names=columns)

    def health_check(self) -> None:
        """Проверяет доступность ClickHouse, выполняя простой запрос."""
        self.client.query("SELECT 1")
