"""
Configuration helpers for ingestion components.

Здесь описаны dataclass-структуры и функции для загрузки конфигурации из
переменных окружения/файлов. В продовой среде их можно расширить, подключив
HashiCorp Vault, AWS Secrets Manager и т.д.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class ExchangeCredentials:
    api_key: str
    api_secret: str
    passphrase: str | None = None


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    schema_registry_url: str | None = None


@dataclass(frozen=True)
class MinIOConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool = True
    bucket: str = "uptrend-raw-store"


def load_exchange_credentials(prefix: str) -> ExchangeCredentials:
    """
    Загружает ключи биржи из переменных окружения.

    Ожидаемые переменные:
        - <PREFIX>_API_KEY
        - <PREFIX>_API_SECRET
        - <PREFIX>_API_PASSPHRASE (опционально)
    """
    env = os.environ
    key = env.get(f"{prefix}_API_KEY")
    secret = env.get(f"{prefix}_API_SECRET")
    if not key or not secret:
        raise RuntimeError(f"credentials for prefix={prefix} not found in environment")
    return ExchangeCredentials(
        api_key=key,
        api_secret=secret,
        passphrase=env.get(f"{prefix}_API_PASSPHRASE"),
    )


def load_kafka_config(env: Mapping[str, str] | None = None) -> KafkaConfig:
    """Возвращает KafkaConfig из окружения."""
    env = env or os.environ
    bootstrap = env.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    registry = env.get("KAFKA_SCHEMA_REGISTRY_URL")
    return KafkaConfig(bootstrap_servers=bootstrap, schema_registry_url=registry)


def load_minio_config(env: Mapping[str, str] | None = None) -> MinIOConfig:
    """Возвращает настройки MinIO/S3."""
    env = env or os.environ
    return MinIOConfig(
        endpoint=env.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=env.get("MINIO_ACCESS_KEY", "minio"),
        secret_key=env.get("MINIO_SECRET_KEY", "minio123"),
        secure=env.get("MINIO_SECURE", "true").lower() == "true",
        bucket=env.get("RAW_BUCKET", "uptrend-raw-store"),
    )
