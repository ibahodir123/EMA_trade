"""
Historical OHLCV loader.

Задачи:
- выгрузить историю свечей с бирж Binance / MEXC / BingX;
- гарантировать полноту временного ряда (gap-check);
- записать данные в Parquet с заданным партиционированием;
- публиковать инсайды о выполнении в систему мониторинга.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Iterable, Sequence

import aiohttp
import pyarrow as pa

from .utils.config import MinIOConfig, load_minio_config
from .utils.gap_check import expected_timestamps, find_gaps
from .utils.storage import ParquetStorage
from .utils.timeframes import timeframe_interval

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoaderConfig:
    exchange: str
    symbol: str
    timeframe: str
    start: datetime
    end: datetime
    batch_size: int = 1000
    storage: MinIOConfig = load_minio_config()

    def interval(self) -> timedelta:
        return timedelta(seconds=timeframe_interval(self.timeframe))


class HistoricalLoader:
    """Высокоуровневый контроллер исторической загрузки."""

    def __init__(self, cfg: LoaderConfig) -> None:
        self.cfg = cfg
        self.storage = ParquetStorage(cfg.storage)

    async def run(self) -> None:
        """Основной цикл загрузки."""
        logger.info(
            "start historical load exchange=%s symbol=%s tf=%s",
            self.cfg.exchange,
            self.cfg.symbol,
            self.cfg.timeframe,
        )
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async for batch in self._fetch_batches(session):
                self._validate_batch(batch)
                await self._write_batch(batch)
        logger.info("finish historical load exchange=%s symbol=%s", self.cfg.exchange, self.cfg.symbol)

    async def _fetch_batches(self, session: aiohttp.ClientSession) -> AsyncIterator[pa.Table]:
        """
        Получает свечи батчами.

        TODO:
            - добавить параметризацию под каждую биржу (endpoint, параметры);
            - реализовать rate-limit и backoff;
            - сохранить позицию (checkpoint) для дозагрузок.
        """
        start_ms = int(self.cfg.start.timestamp() * 1000)
        end_ms = int(self.cfg.end.timestamp() * 1000)
        chunk = self.cfg.batch_size

        current = start_ms
        while current < end_ms:
            params = {
                "symbol": self.cfg.symbol,
                "interval": self.cfg.timeframe,
                "limit": chunk,
                "startTime": current,
                "endTime": min(end_ms, current + chunk * timeframe_interval(self.cfg.timeframe) * 1000),
            }
            url = self._resolve_http_endpoint()
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("exchange=%s http_error status=%s body=%s", self.cfg.exchange, resp.status, text)
                    await asyncio.sleep(1)
                    continue
                data = await resp.json()
            if not data:
                break
            table = self._convert_to_table(data)
            yield table
            last_ts = table.column("timestamp")[-1].as_py()
            current = int(last_ts * 1000) + timeframe_interval(self.cfg.timeframe) * 1000

    def _resolve_http_endpoint(self) -> str:
        """Возвращает REST endpoint в зависимости от биржи."""
        if self.cfg.exchange.upper() == "BINANCE":
            return "https://api.binance.com/api/v3/klines"
        if self.cfg.exchange.upper() == "MEXC":
            return "https://api.mexc.com/api/v3/klines"
        if self.cfg.exchange.upper() == "BINGX":
            return "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
        raise ValueError(f"unsupported exchange {self.cfg.exchange}")

    def _convert_to_table(self, data: Iterable[Iterable]) -> pa.Table:
        """Преобразует сырой ответ API в PyArrow Table."""
        rows = []
        for item in data:
            # Binance-style response: [ openTime, open, high, low, close, volume, closeTime, ... ]
            open_time = item[0] / 1000
            rows.append(
                {
                    "timestamp": int(open_time),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                    "exchange": self.cfg.exchange.upper(),
                    "symbol": self.cfg.symbol.upper(),
                    "tf": self.cfg.timeframe,
                    "date": datetime.fromtimestamp(open_time, tz=timezone.utc).strftime("%Y-%m-%d"),
                }
            )
        return pa.Table.from_pylist(rows)

    def _validate_batch(self, batch: pa.Table) -> None:
        """
        Проверяет консистентность данных (gap-check, сортировка).

        TODO:
            - инициировать повторную загрузку при обнаружении пропуска.
        """
        logger.debug("validate batch rows=%s", batch.num_rows)
        timestamps = [datetime.fromtimestamp(ts.as_py(), tz=timezone.utc) for ts in batch.column("timestamp")]
        if not timestamps:
            return
        interval = self.cfg.interval()
        expected = expected_timestamps(timestamps[0], timestamps[-1], interval)
        gaps = find_gaps(timestamps, expected)
        if gaps:
            logger.warning(
                "gap detected exchange=%s symbol=%s tf=%s gaps=%s",
                self.cfg.exchange,
                self.cfg.symbol,
                self.cfg.timeframe,
                gaps[:5],
            )

    async def _write_batch(self, batch: pa.Table) -> None:
        """
        Записывает Parquet в объектное хранилище.

        TODO:
            - использовать двуфазную запись (tmp + commit);
            - log метрики (объём данных, скорость).
        """
        logger.debug("write batch rows=%s", batch.num_rows)
        self.storage.write_dataset(batch, ["exchange", "symbol", "tf", "date"])


async def main(configs: Sequence[LoaderConfig]) -> None:
    """Точка входа при запуске из CLI."""
    tasks = [HistoricalLoader(cfg).run() for cfg in configs]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    raise SystemExit("Use poetry run python -m ingestion.historical_loader via orchestrator")
