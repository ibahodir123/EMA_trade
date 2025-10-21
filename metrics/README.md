# Metrics Engine (DEV-2)

## ÷ель
–ассчитать все метрики из “« (EMA 20/50/100 и производные показатели) на таймфреймах 5m/1h/4h/1d, использу€ очищенные данные из `raw_store`, и записать результаты в ClickHouse/TimescaleDB дл€ Rule Engine и аналитики.

## ѕлан реализации

1. **—хема витрин**
   - “аблицы `metrics_5m`, `metrics_1h`, `metrics_4h`, `metrics_1d`.
   -  олонки: `exchange`, `symbol`, `timestamp`, `ema*`, `angle_*`, `velocity_*`, `acceleration_*`, `duration_*`, `deviation_*`, `separation_*`, `bar_range`, `close_position`, `calculated_at`.
   - ClickHouse: primary key `(symbol, timestamp)`, партиционирование по дате.

2. **Airflow DAGи**
   - `metrics_calculation_daily` Ч ночной пересчЄт за предыдущий день.
   - `metrics_calculation_incremental` Ч запуск каждые 15 минут дл€ досчЄта новых свечей.
   - »спользуют PyArrow/Pandas дл€ выборочного чтени€ Parquet.

3. **–асчЄтный модуль**
   - `metrics/calculator.py` Ч вычисл€ет EMA и производные метрики, параметры N/M берЄт из `rule_engine_config.yaml`.
   - »нкрементальный режим: дозаписывает метрики на новых барах с учЄтом предыдущего состо€ни€.

4. ** онтроль качества**
   - ёнит-тесты (ta-lib/pandas_ta), кросс-TF проверки.
   - Health-check Vault и ClickHouse перед запуском.
   - Prometheus-метрики: `metrics_calc_duration`, `metrics_calc_rows`, `metrics_calc_failures`.

5. **»нтеграци€**
   - SQL/REST API дл€ Rule Engine.
   - ”правление конфигураци€ми и секретами через Vault.

## —труктура
- `calculator.py` Ч расчЄт метрик.
- `config_loader.py` Ч загрузка параметров N/M.
- `jobs/` Ч orchestrator: чтение Parquet, запись в ClickHouse, загрузка секретов.
- `tests/` Ч юнит-тесты.
- `dags/metrics_calculation_*` Ч Airflow DAGТи.
- `infrastructure/metrics_schema.sql` Ч схема ClickHouse.
