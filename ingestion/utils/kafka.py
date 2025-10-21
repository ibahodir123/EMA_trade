"""
Kafka helpers.

Вынесены функции для инициализации продюсера/консюмера и регистрации схем.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .config import KafkaConfig


async def create_producer(cfg: KafkaConfig) -> AIOKafkaProducer:
    """Создаёт и возвращает AIOKafkaProducer."""
    producer = AIOKafkaProducer(
        bootstrap_servers=cfg.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    return producer


async def create_consumer(cfg: KafkaConfig, topic: str, group_id: str) -> AIOKafkaConsumer:
    """Создаёт AIOKafkaConsumer с json десериализатором."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=cfg.bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
    )
    await consumer.start()
    return consumer


def build_ohlcv_payload(
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    candle: Dict[str, Any],
) -> Dict[str, Any]:
    """Формирует Envelope для публикации в Kafka."""
    return {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "data": candle,
    }
