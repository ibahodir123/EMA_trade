# Uptrend Strategy Repository

## ��������� �������
```
/
+-- config/                 # ���������������� ����� (rule engine � �����������)
+-- dags/                   # Airflow DAG'� ��� ingestion � ������� ������
+-- docs/                   # ����������� ������������ � ������� �������
+-- infrastructure/         # �������/����� �������������� (ClickHouse, Kafka, etc.)
+-- ingestion/              # DEV-1: ���� ������ (������������ � ���������)
+-- metrics/                # DEV-2: ������ ������ � �������� � �������
+-- rule_engine/            # DEV-3/DEV-4: ���������� Rule Engine (stub)
+-- requirements.txt        # Python �����������
L-- README.md               # ���� ����
```

## ������� �����
- **DEV-1 (Data Ingestion)**: ����������� ������ ������������ ��������, ���������� ������������� � ������ Parquet. Airflow DAG'� `ohlcv_backfill` � `ohlcv_incremental` ��������� ��������������� ���������.
- **DEV-2 (Metrics Engine)**: ����������� ������ ������ (EMA + �����������), ������� � ClickHouse, ��������������� DAG'� `metrics_calculation_daily` � `metrics_calculation_incremental`, � ����� ����� ������.

## ������
1. ���������� �����������: `pip install -r requirements.txt`.
2. ��������� ���������� ��������� (������ � MinIO/S3, ClickHouse, Vault).
3. ��������� DAG'� � Airflow � ��������� ��������� `ohlcv_*` � `metrics_*`.

## �����
```
pytest metrics/tests
```

## ������������
�������� �� � ������ ��������� ��������� � ����� `docs/`.
