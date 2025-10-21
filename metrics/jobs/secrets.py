"""
Загрузка секретов из Vault/KMS (пока упрощённый вариант).
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class VaultError(RuntimeError):
    pass


@dataclass(frozen=True)
class ClickHouseSecret:
    host: str
    port: int
    username: str
    password: str


def load_clickhouse_secret() -> ClickHouseSecret:
    """
    Загружает креды ClickHouse из переменных окружения.
    TODO: заменить на реальный вызов Vault.
    """
    host = os.environ.get("CLICKHOUSE_HOST")
    port = int(os.environ.get("CLICKHOUSE_PORT", "9440"))
    username = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")

    if not host:
        raise VaultError("CLICKHOUSE_HOST not set")

    return ClickHouseSecret(host=host, port=port, username=username, password=password)
