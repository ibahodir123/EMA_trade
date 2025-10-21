"""
Storage helpers for MinIO/S3.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from .config import MinIOConfig


class ParquetStorage:
    """Клиент для записи таблиц в MinIO/S3."""

    def __init__(self, cfg: MinIOConfig) -> None:
        self.cfg = cfg
        self.fs = s3fs.S3FileSystem(
            endpoint_url=f"{'https' if cfg.secure else 'http'}://{cfg.endpoint}",
            key=cfg.access_key,
            secret=cfg.secret_key,
        )

    def write_table(self, table: pa.Table, partition_path: str) -> str:
        """Записывает таблицу в указанный путь."""
        output_path = f"{self.cfg.bucket}/{partition_path}/batch-{int(datetime.utcnow().timestamp())}.parquet"
        pq.write_table(table, f"s3://{output_path}", filesystem=self.fs)
        return output_path

    def write_dataset(self, table: pa.Table, partition_cols: Iterable[str]) -> None:
        pq.write_to_dataset(
            table,
            root_path=f"s3://{self.cfg.bucket}",
            partition_cols=list(partition_cols),
            filesystem=self.fs,
            existing_data_behavior="overwrite_or_ignore",
        )
