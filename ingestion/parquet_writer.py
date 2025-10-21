"""
Kafka → Parquet writer.

Принимает сообщения из топиков `ohlcv.*.raw`, превращает их в Parquet-файлы с нужным
партиционированием и складывает в MinIO/S3.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

import pyarrow as pa

from .utils.config import KafkaConfig, MinIOConfig, load_kafka_config, load_minio_config
from .utils.kafka import create_consumer
from .utils.storage import ParquetStorage

logger = logging.getLogger(__name__)


@dataclass
class WriterConfig:
    kafka: KafkaConfig = load_kafka_config()
    storage: MinIOConfig = load_minio_config()
    topic: str
    flush_interval: int = 1000  # кол-во сообщений на партицию
    flush_timeout_s: int = 60   # макс. время между флешами


class ParquetWriter:
    """Консюмер Kafka, который формирует Parquet данные."""

    def __init__(self, cfg: WriterConfig) -> None:
        self.cfg = cfg
        self._buffers: Dict[str, list[Dict[str, Any]]] = defaultdict(list)
        self._last_flush: Dict[str, datetime] = defaultdict(lambda: datetime.now(tz=timezone.utc))
        self._storage = ParquetStorage(cfg.storage)

    async def run(self) -> None:
        consumer = await create_consumer(self.cfg.kafka, self.cfg.topic, group_id="parquet-writer")
        try:
            async for msg in consumer:
                await self._handle_message(msg.value)
        finally:
            await consumer.stop()

    async def _handle_message(self, value: Dict[str, Any]) -> None:
        partition_key = self._partition_key(value)
        self._buffers[partition_key].append(value)
        if self._should_flush(partition_key):
            await self._flush(partition_key)

    def _should_flush(self, partition_key: str) -> bool:
        buffer_len = len(self._buffers[partition_key])
        time_elapsed = (datetime.now(tz=timezone.utc) - self._last_flush[partition_key]).total_seconds()
        return buffer_len >= self.cfg.flush_interval or time_elapsed >= self.cfg.flush_timeout_s

    async def _flush(self, partition_key: str) -> None:
        rows = self._buffers.pop(partition_key, [])
        if not rows:
            return
        table = self._build_table(rows)
        output_path = self._resolve_output_path(partition_key)
        logger.info("flush rows=%s to %s", len(rows), output_path)
        output_path = self._storage.write_table(table, partition_key)
        logger.debug("written parquet path=%s", output_path)
        self._last_flush[partition_key] = datetime.now(tz=timezone.utc)

    def _build_table(self, rows: Iterable[Dict[str, Any]]) -> pa.Table:
        records = []
        for row in rows:
            candle = row["data"]
            records.append(
                {
                    "exchange": row["exchange"],
                    "symbol": row["symbol"],
                    "timeframe": row["timeframe"],
                    "timestamp": candle["ts"],
                    "open": candle["open"],
                    "high": candle["high"],
                    "low": candle["low"],
                    "close": candle["close"],
                    "volume": candle["volume"],
                    "ingested_at": row["ingested_at"],
                }
            )
        return pa.Table.from_pylist(records)

    def _partition_key(self, value: Dict[str, Any]) -> str:
        candle_ts = datetime.fromtimestamp(value["data"]["ts"], tz=timezone.utc)
        date_str = candle_ts.strftime("%Y-%m-%d")
        return f"{value['exchange']}/{value['symbol']}/{value['timeframe']}/{date_str}"


async def main() -> None:
    writer = ParquetWriter(
        WriterConfig(topic="ohlcv.5m.raw")
    )
    await writer.run()


if __name__ == "__main__":
    raise SystemExit("Run via supervisor or container orchestrator")
