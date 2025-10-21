# Kafka Topics (DEV-1)

| Topic             | Purpose                      | Retention | Partitions | Notes |
| ----------------- | --------------------------- | --------- | ---------- | ----- |
| ohlcv.5m.raw      | Raw 5m candles stream       | 7 days    | 6          | Used by parquet writer |
| ohlcv.1h.raw      | Raw 1h candles stream       | 14 days   | 6          | Downstream metrics engine |
| ohlcv.4h.raw      | Raw 4h candles stream       | 30 days   | 3          | Low volume |
| ohlcv.1d.raw      | Raw daily candles stream    | 90 days   | 3          | Archival feed |

- Replication factor: 3
- Cleanup policy: `delete`
- Schema Registry: Avro schema `ohlcv-value` (timestamp, open, high, low, close, volume)
