"""
Модуль расчёта метрик для Metrics Engine.

Принимает DataFrame со свечами (timestamp, open, high, low, close, volume)
и вычисляет:
    - EMA 20/50/100;
    - производные показатели (Angle, Velocity, Acceleration, Duration);
    - расстояния (Deviation, Separation);
    - BarRange и ClosePosition.

Рассчитанные значения возвращаются в виде DataFrame, готового к загрузке в витрины.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List

import numpy as np
import pandas as pd

from .config_loader import MetricParams

try:
    import talib  # type: ignore
except ImportError:  # pragma: no cover
    talib = None


EMA_PERIODS = (20, 50, 100)


@dataclass
class MetricsResult:
    """Структура для возвращения рассчитанных данных."""

    dataframe: pd.DataFrame


def calculate_metrics(
    candles: pd.DataFrame,
    timeframe: str,
    params: MetricParams,
) -> MetricsResult:
    """
    Вычисляет метрики для указанного таймфрейма.

    candles: DataFrame с колонками timestamp (UTC секундный), open, high, low, close, volume.
    timeframe: строка вида "5m", "1h", "4h", "1d".
    params: параметры N/M из конфигурации.
    """
    df = candles.sort_values("timestamp").copy()

    _compute_ema(df)
    _compute_angle(df, timeframe, params.n_angle)
    _compute_velocity(df, timeframe, params.n_velocity)
    _compute_acceleration(df)
    _compute_duration(df, timeframe, params.min_duration_bars)
    _compute_deviation(df)
    _compute_separation(df)
    _compute_bar_features(df)

    df["calculated_at"] = pd.Timestamp.utcnow()

    return MetricsResult(dataframe=df)


def _compute_ema(df: pd.DataFrame) -> None:
    for period in EMA_PERIODS:
        col = f"ema{period}"
        if talib is not None:
            df[col] = talib.EMA(df["close"].astype(float).values, timeperiod=period)
        else:  # fallback
            df[col] = df["close"].ewm(span=period, adjust=False).mean()


def _compute_angle(df: pd.DataFrame, timeframe: str, n_map: Dict[str, int]) -> None:
    n = n_map.get(timeframe)
    if not n:
        raise KeyError(f"Отсутствует параметр N_ANGLE для {timeframe}")
    for period in EMA_PERIODS:
        ema_col = f"ema{period}"
        df[f"angle_{period}"] = (df[ema_col] - df[ema_col].shift(n)) / n


def _compute_velocity(df: pd.DataFrame, timeframe: str, n_map: Dict[str, int]) -> None:
    n = n_map.get(timeframe)
    if not n:
        raise KeyError(f"Отсутствует параметр N_VELOCITY для {timeframe}")
    for period in EMA_PERIODS:
        ema_col = f"ema{period}"
        df[f"velocity_{period}"] = df[ema_col] - df[ema_col].shift(n)


def _compute_acceleration(df: pd.DataFrame) -> None:
    for period in EMA_PERIODS:
        vel_col = f"velocity_{period}"
        df[f"acceleration_{period}"] = df[vel_col] - df[vel_col].shift(1)


def _compute_duration(df: pd.DataFrame, timeframe: str, min_bars_map: Dict[str, int]) -> None:
    min_bars = min_bars_map.get(timeframe)
    if not min_bars:
        raise KeyError(f"Отсутствует параметр MIN_DURATION_BARS для {timeframe}")
    for period in EMA_PERIODS:
        ema_col = f"ema{period}"
        direction = np.where(df[ema_col].diff() >= 0, 1, -1)
        duration = np.zeros(len(df), dtype=int)
        count = 0
        last_dir = 0
        for idx, dir_value in enumerate(direction):
            if dir_value == last_dir:
                count += 1
            else:
                count = 1
                last_dir = dir_value
            duration[idx] = count if dir_value > 0 else -count
        df[f"duration_{period}"] = duration
        df[f"duration_{period}"] = df[f"duration_{period}"].where(
            df[f"duration_{period}"].abs() >= min_bars, 0
        )


def _compute_deviation(df: pd.DataFrame) -> None:
    for period in EMA_PERIODS:
        ema_col = f"ema{period}"
        df[f"dev_{period}"] = (df["close"] - df[ema_col]) / df[ema_col] * 100


def _compute_separation(df: pd.DataFrame) -> None:
    pairs = [(20, 50), (50, 100), (20, 100)]
    for small, large in pairs:
        col = f"sep_{small}_{large}"
        df[col] = (df[f"ema{small}"] - df[f"ema{large}"]) / df[f"ema{large}"] * 100


def _compute_bar_features(df: pd.DataFrame) -> None:
    df["bar_range"] = df["high"] - df["low"]
    denom = df["high"] - df["low"]
    df["close_position"] = np.where(
        denom != 0,
        (df["close"] - df["low"]) / denom,
        0.5,
    )
