# Metrics Engine (DEV-2)

## ����
���������� ��� ������� �� �� (EMA 20/50/100 � ����������� ����������) �� ����������� 5m/1h/4h/1d, ��������� ��������� ������ �� `raw_store`, � �������� ���������� � ClickHouse/TimescaleDB ��� Rule Engine � ���������.

## ���� ����������

1. **����� ������**
   - ������� `metrics_5m`, `metrics_1h`, `metrics_4h`, `metrics_1d`.
   - �������: `exchange`, `symbol`, `timestamp`, `ema*`, `angle_*`, `velocity_*`, `acceleration_*`, `duration_*`, `deviation_*`, `separation_*`, `bar_range`, `close_position`, `calculated_at`.
   - ClickHouse: primary key `(symbol, timestamp)`, ����������������� �� ����.

2. **Airflow DAG�**
   - `metrics_calculation_daily` � ������ �������� �� ���������� ����.
   - `metrics_calculation_incremental` � ������ ������ 15 ����� ��� ������� ����� ������.
   - ���������� PyArrow/Pandas ��� ����������� ������ Parquet.

3. **��������� ������**
   - `metrics/calculator.py` � ��������� EMA � ����������� �������, ��������� N/M ���� �� `rule_engine_config.yaml`.
   - ��������������� �����: ������������ ������� �� ����� ����� � ������ ����������� ���������.

4. **�������� ��������**
   - ����-����� (ta-lib/pandas_ta), �����-TF ��������.
   - Health-check Vault � ClickHouse ����� ��������.
   - Prometheus-�������: `metrics_calc_duration`, `metrics_calc_rows`, `metrics_calc_failures`.

5. **����������**
   - SQL/REST API ��� Rule Engine.
   - ���������� �������������� � ��������� ����� Vault.

## ���������
- `calculator.py` � ������ ������.
- `config_loader.py` � �������� ���������� N/M.
- `jobs/` � orchestrator: ������ Parquet, ������ � ClickHouse, �������� ��������.
- `tests/` � ����-�����.
- `dags/metrics_calculation_*` � Airflow DAG��.
- `infrastructure/metrics_schema.sql` � ����� ClickHouse.
