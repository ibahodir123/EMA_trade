"""
Job-уровень: чтение свечей, вычисление метрик, запись в ClickHouse.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Tuple

import pandas as pd
from prometheus_client import Counter, Histogram

from .clickhouse_client import ClickHouseConfig, ClickHouseWriter
from .parquet_reader import RawParquetReader, RawStoreConfig
from ..calculator import calculate_metrics
from ..config_loader import MetricParams, load_metric_params


METRICS_DURATION = Histogram("metrics_calc_duration_seconds", "Расчёт метрик, секунды")
METRICS_ROWS = Counter("metrics_calc_rows_total", "Количество обработанных строк", ["timeframe"])
METRICS_FAILURES = Counter("metrics_calc_failures_total", "Счётчик ошибок", ["stage"])


@dataclass
class JobConfig:
    exchanges: Iterable[str]
    symbols: Iterable[str]
    timeframes: Iterable[str]
    raw_store: RawStoreConfig
    clickhouse: ClickHouseConfig


class MetricsCalculationJob:
    """Инкапсулирует запуск расчёта метрик."""

    def __init__(self, job_cfg: JobConfig, metric_params: MetricParams | None = None) -> None:
        self.job_cfg = job_cfg
        self.metric_params = metric_params or load_metric_params()
        self.reader = RawParquetReader(job_cfg.raw_store)
        self.writer = ClickHouseWriter(job_cfg.clickhouse)

    def run_full(self, *, start_ts: int | None = None, end_ts: int | None = None) -> None:
        """Полный пересчёт (или окно start/end)."""
        for exchange in self.job_cfg.exchanges:
            for symbol in self.job_cfg.symbols:
                for tf in self.job_cfg.timeframes:
                    self._process_pair(exchange, symbol, tf, start_ts=start_ts, end_ts=end_ts)

    def run_incremental(self, *, lookback_seconds: int = 86400) -> None:
        """Инкрементальный режим: читаем свечи за lookback_seconds."""
        end_ts = int(datetime.utcnow().timestamp())
        start_ts = end_ts - lookback_seconds
        self.run_full(start_ts=start_ts, end_ts=end_ts)

    def _process_pair(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        *,
        start_ts: int | None,
        end_ts: int | None,
    ) -> None:
        with METRICS_DURATION.time():
            try:
                candles = self.reader.load(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
                if candles.empty:
                    return
                result = calculate_metrics(candles, timeframe, self.metric_params)
                METRICS_ROWS.labels(timeframe=timeframe).inc(len(result.dataframe))
                table = f"metrics_{timeframe}"
                self.writer.insert_dataframe(table, result.dataframe)
            except Exception as exc:
                METRICS_FAILURES.labels(stage="calc").inc()
                raise exc
