import pandas as pd

from metrics.calculator import calculate_metrics
from metrics.config_loader import MetricParams


def test_calculate_metrics_basic():
    params = MetricParams(
        n_angle={"5m": 5},
        n_velocity={"5m": 5},
        min_duration_bars={"5m": 3},
    )
    candles = pd.DataFrame(
        {
            "timestamp": pd.RangeIndex(start=0, stop=30),
            "open": [100 + x for x in range(30)],
            "high": [101 + x for x in range(30)],
            "low": [99 + x for x in range(30)],
            "close": [100 + x for x in range(30)],
            "volume": [10.0] * 30,
        }
    )
    result = calculate_metrics(candles, "5m", params).dataframe
    assert "ema20" in result.columns
    assert "angle_20" in result.columns
    assert "velocity_20" in result.columns
    assert "acceleration_20" in result.columns
    assert "duration_20" in result.columns
    assert "dev_20" in result.columns
    assert "sep_20_50" in result.columns
    assert "bar_range" in result.columns
    assert "close_position" in result.columns

import numpy as np
import talib


def test_ema_matches_talib():
    params = MetricParams(
        n_angle={"5m": 5},
        n_velocity={"5m": 5},
        min_duration_bars={"5m": 3},
    )
    close = np.linspace(100, 110, 100)
    candles = pd.DataFrame(
        {
            "timestamp": pd.RangeIndex(0, 100),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": np.ones(100),
        }
    )
    result = calculate_metrics(candles, "5m", params).dataframe
    talib_ema20 = talib.EMA(close, timeperiod=20)
    # Сравниваем последние значения, игнорируя первые NaN
    assert np.allclose(result["ema20"].iloc[19:], talib_ema20[19:], atol=1e-6)


def test_cross_tf_consistency():
    params = MetricParams(
        n_angle={"5m": 5, "1h": 5},
        n_velocity={"5m": 5, "1h": 5},
        min_duration_bars={"5m": 3, "1h": 1},
    )
    # 12 * 5m = 1h
    base = np.arange(0, 120)
    candles_5m = pd.DataFrame(
        {
            "timestamp": base,
            "open": base,
            "high": base + 1,
            "low": base - 1,
            "close": base,
            "volume": np.ones_like(base),
        }
    )
    candles_1h = candles_5m.groupby(candles_5m.index // 12).agg(
        {
            "timestamp": "last",
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    ).reset_index(drop=True)

    result_5m = calculate_metrics(candles_5m, "5m", params).dataframe
    result_1h = calculate_metrics(candles_1h, "1h", params).dataframe

    # EMA20 на часе должна совпасть с EMA, рассчитанной по агрегированным данным
    assert np.allclose(
        result_1h["ema20"].iloc[10:],
        result_5m.groupby(result_5m.index // 12)["ema20"].last().iloc[10:],
        atol=1e-6,
    )
