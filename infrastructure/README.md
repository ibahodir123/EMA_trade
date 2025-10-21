# DEV-1 Infrastructure Notes

## Services

| Service  | Purpose                            | Deployment hint                |
| -------- | ---------------------------------- | ------------------------------ |
| MinIO    | Object storage for raw Parquet     | Helm chart `minio/minio`       |
| Kafka    | Message bus for streaming OHLCV    | Strimzi / Confluent operators  |
| Airflow  | Orchestrator for batch pipelines   | Managed service / Helm chart   |
| Prometheus/Grafana | Monitoring and alerting | kube-prometheus stack          |

## Topics

- `ohlcv.5m.raw`
- `ohlcv.1h.raw`
- `ohlcv.4h.raw`
- `ohlcv.1d.raw`

Все топики с репликацией 3, cleanup policy `delete`, retention 7 дней.

## Parquet layout

```
s3://uptrend-raw-store/
  exchange=BINANCE/
    symbol=BTCUSDT/
      tf=5m/
        date=2025-01-01/
          batch-*.parquet
```

## Security

- API ключи хранятся в HashiCorp Vault (path `secret/ingestion/<exchange>`).
- Сервисы получают доступ через Vault Agent или env-injector.

## Monitoring

- Экспортируемые метрики: `ingestion_gap_count`, `ingestion_kafka_lag`, `ingestion_write_latency`.
- Алерты: gap > 3 свечей, отсутствие данных > 2 минут, Kafka lag > 500 сообщений.
