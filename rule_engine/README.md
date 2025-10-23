# Rule Engine (DEV-3/DEV-4)

Stub implementation now lives in rule_engine/processor.py. It demonstrates how the future trading logic will pull the latest metrics from ClickHouse, apply the thresholds from config/rule_engine_config.yaml, and produce BUY/HOLD signals. The Airflow DAG dags/trading_signals_daily.py orchestrates the stub right after metrics_calculation_daily completes.
