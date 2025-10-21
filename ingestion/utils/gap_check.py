"""
Gap check utilities.

Определяет недостающие временные интервалы в OHLCV-рядe.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, List


def expected_timestamps(start: datetime, end: datetime, interval: timedelta) -> List[datetime]:
    """Возвращает список ожидаемых меток времени."""
    cursor = start.astimezone(timezone.utc)
    end = end.astimezone(timezone.utc)
    result: List[datetime] = []
    while cursor <= end:
        result.append(cursor)
        cursor += interval
    return result


def find_gaps(existing: Iterable[datetime], expected: Iterable[datetime]) -> List[datetime]:
    """
    Возвращает список недостающих timestamp.

    TODO:
        - оптимизировать для больших наборов (использовать множества);
        - интегрировать с алертингом.
    """
    existing_set = {ts.astimezone(timezone.utc) for ts in existing}
    return [ts for ts in expected if ts not in existing_set]
