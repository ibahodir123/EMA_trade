"""Rule Engine processor stub for DEV-3."""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import yaml
from clickhouse_connect import get_client

from metrics.jobs.airflow_integration import (
    DEFAULT_CLICKHOUSE_CONN_ID,
    build_clickhouse_config,
)
from metrics.jobs.clickhouse_client import ClickHouseConfig

logger = logging.getLogger(__name__)

CONFIG_PATH_ENV = "RULE_ENGINE_CONFIG_PATH"
SYMBOLS_ENV = "RULE_ENGINE_SYMBOLS"
EXCHANGES_ENV = "RULE_ENGINE_EXCHANGES"

DEFAULT_CONFIG_PATH = "config/rule_engine_config.yaml"


@dataclass
class Signal:
    exchange: str
    symbol: str
    timeframe: str
    action: str
    reason: str
    generated_at: datetime

    def asdict(self) -> Dict[str, object]:  # convenience for XCom/serialization
        return asdict(self)


def load_rule_engine_config(path: str | Path = DEFAULT_CONFIG_PATH) -> Dict[str, object]:
    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError(f"Rule Engine config not found: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp) or {}


class RuleEngineProcessor:
    """Minimal processor that fetches metrics from ClickHouse and applies simple rules."""

    def __init__(self, clickhouse_cfg: ClickHouseConfig, rules: Dict[str, object]) -> None:
        self.cfg = clickhouse_cfg
        self.rules = rules
        self.client = get_client(
            host=clickhouse_cfg.host,
            port=clickhouse_cfg.port,
            username=clickhouse_cfg.username,
            password=clickhouse_cfg.password,
            secure=clickhouse_cfg.secure,
            database=clickhouse_cfg.database,
        )

    def evaluate(self, exchanges: Iterable[str], symbols: Iterable[str]) -> List[Signal]:
        signals: List[Signal] = []
        for exchange in exchanges:
            for symbol in symbols:
                metrics = {
                    tf: self._fetch_latest_metric(exchange, symbol, tf)
                    for tf in ("1d", "4h", "1h", "5m")
                }
                action, reason = self._apply_rules(metrics)
                signal = Signal(
                    exchange=exchange,
                    symbol=symbol,
                    timeframe="5m",
                    action=action,
                    reason=reason,
                    generated_at=datetime.utcnow(),
                )
                signals.append(signal)
                logger.info(
                    "rule_engine signal exchange=%s symbol=%s action=%s reason=%s",
                    exchange,
                    symbol,
                    action,
                    reason,
                )
        return signals

    def _fetch_latest_metric(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> Optional[pd.Series]:
        table = f"metrics_{timeframe}"
        query = (
            "SELECT * FROM {table} "
            "WHERE exchange = %(exchange)s AND symbol = %(symbol)s "
            "ORDER BY timestamp DESC LIMIT 1"
        ).format(table=table)

        try:
            df = self.client.query_df(query, parameters={"exchange": exchange, "symbol": symbol})
        except Exception as exc:  # pragma: no cover - ClickHouse errors surfaced at runtime
            logger.warning("failed to load metrics table=%s error=%s", table, exc)
            return None

        if df.empty:
            return None
        return df.iloc[0]

    def _apply_rules(self, metrics: Dict[str, Optional[pd.Series]]) -> tuple[str, str]:
        thresholds = self.rules.get("RULE_ENGINE_THRESHOLDS", {})

        missing = [tf for tf, data in metrics.items() if data is None]
        if missing:
            return "HOLD", f"missing:{','.join(missing)}"

        reasons: List[str] = []
        filter_1d = thresholds.get("FILTER_1D", {})
        if filter_1d:
            angle_threshold = filter_1d.get("ANGLE_100_MIN")
            if angle_threshold is not None and metrics["1d"].get("angle_100", 0) < angle_threshold:
                reasons.append("angle_100_low")
            duration_threshold = filter_1d.get("DURATION_100_MIN")
            if duration_threshold is not None and metrics["1d"].get("duration_100", 0) < duration_threshold:
                reasons.append("duration_100_short")

        filter_4h = thresholds.get("FILTER_4H", {})
        if filter_4h.get("EMA_ORDER_REQUIRED"):
            ema20 = metrics["4h"].get("ema20")
            ema50 = metrics["4h"].get("ema50")
            ema100 = metrics["4h"].get("ema100")
            if not (ema20 and ema50 and ema100 and ema20 > ema50 > ema100):
                reasons.append("ema_order_break")
        sep_min = filter_4h.get("SEP_50_20_MIN")
        if sep_min is not None and metrics["4h"].get("sep_50_20", 0) < sep_min:
            reasons.append("sep_50_20_low")

        filter_1h = thresholds.get("FILTER_1H", {})
        correction_min = filter_1h.get("CORRECTION_DURATION_MIN")
        if correction_min is not None and abs(metrics["1h"].get("duration_20", 0)) < correction_min:
            reasons.append("correction_duration_short")

        trigger_5m = thresholds.get("TRIGGER_5M", {})
        sep_max = trigger_5m.get("SEP_50_20_MAX")
        if sep_max is not None and metrics["5m"].get("sep_50_20", 0) > sep_max:
            reasons.append("sep_5m_high")
        velocity_min = trigger_5m.get("VELOCITY_20_MIN")
        if velocity_min is not None and metrics["5m"].get("velocity_20", 0) < velocity_min:
            reasons.append("velocity_20_low")
        bar_range_coef = trigger_5m.get("BAR_RANGE_MAX_COEF")
        if bar_range_coef is not None:
            bar_range = metrics["5m"].get("bar_range")
            ema20 = metrics["5m"].get("ema20")
            if bar_range is not None and ema20:
                if bar_range > bar_range_coef * abs(ema20) * 0.01:
                    reasons.append("bar_range_wide")

        if reasons:
            return "HOLD", ";".join(reasons)
        return "BUY", "conditions_met"


def generate_trading_signals(
    *,
    execution_dt: Optional[datetime] = None,
    clickhouse_conn_id: str = DEFAULT_CLICKHOUSE_CONN_ID,
    config_path: str = DEFAULT_CONFIG_PATH,
    exchanges: Optional[Iterable[str]] = None,
    symbols: Optional[Iterable[str]] = None,
) -> List[Signal]:
    config = load_rule_engine_config(config_path)
    global_cfg = config.get("GLOBAL", {})

    if exchanges is None:
        exchanges = _parse_cli_list(
            os.environ.get(EXCHANGES_ENV),
            fallback=list(global_cfg.get("EXCHANGES", [])) or ["BINANCE"],
        )
    else:
        exchanges = list(exchanges)

    if symbols is None:
        symbols = _parse_cli_list(
            os.environ.get(SYMBOLS_ENV),
            fallback=["BTCUSDT", "ETHUSDT"],
        )
    else:
        symbols = list(symbols)

    clickhouse_cfg = build_clickhouse_config(conn_id=clickhouse_conn_id)
    processor = RuleEngineProcessor(clickhouse_cfg, config)
    signals = processor.evaluate(exchanges, symbols)

    if execution_dt:
        logger.info("rule_engine generated %d signals for %s", len(signals), execution_dt)
    else:
        logger.info("rule_engine generated %d signals", len(signals))

    return signals


def _parse_cli_list(raw: Optional[str], fallback: List[str]) -> List[str]:
    if not raw:
        return list(fallback)
    return [item.strip() for item in raw.split(",") if item.strip()]
