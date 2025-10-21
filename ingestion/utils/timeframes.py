"""Common timeframe helpers."""

TF_TO_SECONDS = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


def timeframe_interval(tf: str) -> int:
    return TF_TO_SECONDS[tf]
