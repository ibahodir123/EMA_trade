"""
Helpers that bridge Airflow connections with the metrics engine runtime.

The functions in this module intentionally import Airflow providers lazily so that
the core metrics package can stay importable in pure Python contexts (tests, local
experiments) while the DAGs can hydrate runtime configs from Airflow Connections.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import pyarrow.fs as pa_fs

from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from .clickhouse_client import ClickHouseConfig
from .parquet_reader import RawStoreConfig

DEFAULT_S3_CONN_ID = "s3_minio_conn"
DEFAULT_CLICKHOUSE_CONN_ID = "clickhouse_conn"


def _ensure_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def build_raw_store_config(
    *,
    conn_id: str = DEFAULT_S3_CONN_ID,
    default_root_path: Optional[str] = None,
) -> RawStoreConfig:
    """
    Construct RawStoreConfig using credentials defined in an Airflow connection.

    The S3 connection should contain endpoint/credentials in either the login/password
    fields or in ``extra`` as ``aws_access_key_id`` / ``aws_secret_access_key`` /
    ``endpoint_url``. The bucket/path fallback is controlled by ``default_root_path`` or
    the ``RAW_STORE_PATH`` environment variable.
    """
    hook = S3Hook(aws_conn_id=conn_id)
    connection = hook.get_connection(conn_id)

    credentials = hook.get_credentials()
    extra: Dict[str, Any] = connection.extra_dejson or {}

    key = extra.get("aws_access_key_id") or credentials.access_key
    secret = extra.get("aws_secret_access_key") or credentials.secret_key
    token = extra.get("aws_session_token") or credentials.token

    endpoint_url = extra.get("endpoint_url")
    region_name = extra.get("region_name")
    root_path = (
        extra.get("root_path")
        or default_root_path
        or os.environ.get("RAW_STORE_PATH", "s3://uptrend-raw-store")
    )

    filesystem = pa_fs.S3FileSystem(
        access_key=key,
        secret_key=secret,
        session_token=token,
        endpoint_override=endpoint_url,
        region=region_name,
    )

    return RawStoreConfig(root_path=root_path, filesystem=filesystem)


def build_clickhouse_config(
    *,
    conn_id: str = DEFAULT_CLICKHOUSE_CONN_ID,
    default_database: Optional[str] = None,
    secure_default: bool = False,
) -> ClickHouseConfig:
    """
    Construct ClickHouseConfig from an Airflow connection.

    The connection may provide host/port/login/password directly or via ``extra``.
    An optional ``secure`` flag in ``extra`` or environment variable ``CLICKHOUSE_SECURE``
    controls TLS usage.
    """
    connection = BaseHook.get_connection(conn_id)
    extra: Dict[str, Any] = connection.extra_dejson or {}

    secure = _ensure_bool(
        extra.get("secure"),
        default=_ensure_bool(os.environ.get("CLICKHOUSE_SECURE"), default=secure_default),
    )

    port = extra.get("port") or connection.port
    if port is None:
        port = 9440 if secure else 8123

    database = (
        extra.get("database")
        or connection.schema
        or default_database
        or os.environ.get("CLICKHOUSE_DATABASE", "uptrend")
    )

    return ClickHouseConfig(
        host=connection.host or extra.get("host", "localhost"),
        port=int(port),
        username=connection.login or extra.get("username") or "default",
        password=connection.password or extra.get("password") or "",
        secure=secure,
        database=database,
    )
