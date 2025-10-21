"""
Stream listener for real-time OHLCV aggregation.

Компонент подключается к WebSocket API бирж, агрегирует данные в требуемые
таймфреймы и публикует результат в Kafka.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import aiohttp

from .utils.config import KafkaConfig, load_kafka_config
from .utils.kafka import build_ohlcv_payload, create_producer
from .utils.timeframes import TF_TO_SECONDS

logger = logging.getLogger(__name__)

PING_INTERVAL = 30
RECONNECT_DELAY = 5


class StreamListener:
    """Управляет подписками на WebSocket и публикацией в Kafka."""

    def __init__(
        self,
        *,
        kafka_cfg: KafkaConfig,
        topic: str,
        exchange: str,
        symbol: str,
        timeframes: list[str],
    ) -> None:
        self.kafka_cfg = kafka_cfg
        self.topic = topic
        self.exchange = exchange
        self.symbol = symbol
        self.timeframes = timeframes
        self._producer = None
        self._aggregators = {tf: CandlestickAggregator(tf) for tf in timeframes}

    async def run(self) -> None:
        """Основной цикл запуска."""
        self._producer = await create_producer(self.kafka_cfg)
        try:
            while True:
                try:
                    await self._listen_loop()
                except Exception:
                    logger.exception("stream listener crashed, restart after delay")
                    await asyncio.sleep(RECONNECT_DELAY)
        finally:
            if self._producer:
                await self._producer.stop()

    async def _listen_loop(self) -> None:
        """Открывает WebSocket и обрабатывает сообщения."""
        url = self._build_ws_url()
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=PING_INTERVAL) as ws:
                await self._subscribe(ws)
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await self._handle_message(json.loads(msg.data))
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise RuntimeError(f"WebSocket error: {msg.data}")

    async def _handle_message(self, payload: Dict[str, Any]) -> None:
        """
        Обрабатывает raw-данные, обновляет агрегаторы и публикует готовые свечи.

        TODO:
            - адаптеры под разный формат ответов (Binance/MEXC/BingX);
            - дедупликация (skip, если timestamp уже опубликован);
            - REST fallback при пропусках.
        """
        for tf, aggregator in self._aggregators.items():
            candle = aggregator.update(payload)
            if candle:
                await self._publish(tf, candle)

    async def _publish(self, timeframe: str, candle: Dict[str, Any]) -> None:
        """Отправляет candle в Kafka."""
        assert self._producer is not None
        envelope = build_ohlcv_payload(
            exchange=self.exchange,
            symbol=self.symbol,
            timeframe=timeframe,
            candle=candle,
        )
        envelope["ingested_at"] = datetime.now(tz=timezone.utc).isoformat()
        await self._producer.send_and_wait(self.topic, envelope)

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Отправляет подписку на нужные каналы биржи."""
        # TODO: реализовать подписку в зависимости от биржи
        logger.info("subscribe exchange=%s symbol=%s", self.exchange, self.symbol)

    def _build_ws_url(self) -> str:
        """Формирует URL WebSocket для биржи."""
        exchange = self.exchange.upper()
        if exchange == "BINANCE":
            return f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@trade"
        if exchange == "MEXC":
            return f"wss://wbs.mexc.com/raw/ws"
        if exchange == "BINGX":
            return "wss://open-api-ws.bingx.com/openapi/quote/ws/v1"
        raise ValueError(f"unsupported exchange {self.exchange}")


class CandlestickAggregator:
    """Инкрементально собирает свечи из тиков/1m данных."""

    def __init__(self, timeframe: str) -> None:
        self.timeframe = timeframe
        self.bucket_seconds = TF_TO_SECONDS[timeframe]
        self.current_bucket: Optional[int] = None
        self.state: Dict[str, Any] = {}

    def update(self, payload: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Обновляет candle, возвращает готовую свечу при закрытии интервала.

        payload должен содержать значения тика: timestamp, price, volume.
        """
        tick = self._extract_tick(payload)
        if tick is None:
            return None

        bucket = int(tick["timestamp"] // self.bucket_seconds * self.bucket_seconds)
        if self.current_bucket is None:
            self.current_bucket = bucket
            self.state = self._initial_state(bucket, tick)
            return None

        if bucket != self.current_bucket:
            finished = self._build_candle()
            self.current_bucket = bucket
            self.state = self._initial_state(bucket, tick)
            return finished

        self.state["high"] = max(self.state["high"], tick["price"])
        self.state["low"] = min(self.state["low"], tick["price"])
        self.state["close"] = tick["price"]
        self.state["volume"] += tick["volume"]
        return None

    def _extract_tick(self, payload: Dict[str, Any]) -> Dict[str, float] | None:
        """
        Вытаскивает тик из сообщения биржи.

        TODO:
            - поддержка разных форматов: trades, klines, aggTrade и т.п.
        """
        data = payload.get("data") or payload
        ts = data.get("E") or data.get("T") or data.get("timestamp")
        price = data.get("p") or data.get("c") or data.get("price")
        volume = data.get("q") or data.get("v") or data.get("volume")
        if ts is None or price is None or volume is None:
            return None
        return {
            "timestamp": float(ts) / 1000.0,
            "price": float(price),
            "volume": float(volume),
        }

    def _initial_state(self, bucket: int, tick: Dict[str, float]) -> Dict[str, Any]:
        return {
            "ts": bucket,
            "open": tick["price"],
            "high": tick["price"],
            "low": tick["price"],
            "close": tick["price"],
            "volume": tick["volume"],
        }

    def _build_candle(self) -> Dict[str, Any]:
        return dict(self.state)


async def main() -> None:
    listener = StreamListener(
        kafka_cfg=load_kafka_config(),
        topic="ohlcv.5m.raw",
        exchange="BINANCE",
        symbol="BTCUSDT",
        timeframes=["5m", "1h", "4h", "1d"],
    )
    await listener.run()


if __name__ == "__main__":
    raise SystemExit("Use supervisor or process manager to start stream listener")
