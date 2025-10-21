"""
Загрузчик конфигурации для Metrics Engine.

Читает config/rule_engine_config.yaml и возвращает параметры расчёта метрик,
интерпретируя значения N/M для разных таймфреймов.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping

import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_PATH = BASE_DIR / "config" / "rule_engine_config.yaml"


@dataclass(frozen=True)
class MetricParams:
    n_angle: Mapping[str, int]
    n_velocity: Mapping[str, int]
    min_duration_bars: Mapping[str, int]


def load_metric_params(config_path: Path = CONFIG_PATH) -> MetricParams:
    """Читает YAML-конфигурацию и возвращает параметры расчётов."""
    with config_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    try:
        section = data["METRICS_CALCULATION"]
    except KeyError as exc:
        raise KeyError("Секция METRICS_CALCULATION отсутствует в конфиге") from exc

    return MetricParams(
        n_angle=_to_int_map(section["N_ANGLE"]),
        n_velocity=_to_int_map(section["N_VELOCITY"]),
        min_duration_bars=_to_int_map(section["MIN_DURATION_BARS"]),
    )


def _to_int_map(obj: Mapping[str, int]) -> Dict[str, int]:
    """Валидирует и приводит значения к int."""
    result: Dict[str, int] = {}
    for tf, value in obj.items():
        try:
            result[tf] = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Неверное числовое значение для '{tf}': {value}") from exc
    return result
