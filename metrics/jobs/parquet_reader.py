"""
Модуль чтения данных из Parquet raw_store.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional

import pandas as pd
import pyarrow.dataset as ds


@dataclass(frozen=True)
class RawStoreConfig:
    root_path: str = "s3://uptrend-raw-store"
    filesystem: ds.FileSystem | None = None  # будет подставлен s3fs, если нужно


class RawParquetReader:
    """Читает свечи из raw_store с учётом партиционирования."""

    def __init__(self, cfg: RawStoreConfig) -> None:
        self.dataset = ds.dataset(cfg.root_path, filesystem=cfg.filesystem, format="parquet")

    def load(
        self,
        *,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Загружает свечи для заданного интервала."""
        expr = (
            (ds.field("exchange") == exchange.upper())
            & (ds.field("symbol") == symbol.upper())
            & (ds.field("tf") == timeframe)
        )
        if start_ts is not None:
            expr = expr & (ds.field("timestamp") >= start_ts)
        if end_ts is not None:
            expr = expr & (ds.field("timestamp") < end_ts)
        table = self.dataset.to_table(filter=expr)
        df = table.to_pandas()
        if start_ts is not None:
            df = df[df["timestamp"] >= start_ts]
        if end_ts is not None:
            df = df[df["timestamp"] < end_ts]
        return df[["timestamp", "open", "high", "low", "close", "volume"]].sort_values("timestamp")
