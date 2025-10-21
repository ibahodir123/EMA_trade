-- ClickHouse schema for metrics engine tables.

CREATE TABLE IF NOT EXISTS metrics_5m
(
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    timestamp DateTime,
    ema20 Float64,
    ema50 Float64,
    ema100 Float64,
    angle_20 Float64,
    angle_50 Float64,
    angle_100 Float64,
    velocity_20 Float64,
    velocity_50 Float64,
    velocity_100 Float64,
    acceleration_20 Float64,
    acceleration_50 Float64,
    acceleration_100 Float64,
    duration_20 Int32,
    duration_50 Int32,
    duration_100 Int32,
    dev_20 Float64,
    dev_50 Float64,
    dev_100 Float64,
    sep_20_50 Float64,
    sep_50_100 Float64,
    sep_20_100 Float64,
    bar_range Float64,
    close_position Float64,
    calculated_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
PRIMARY KEY (symbol, timestamp)
ORDER BY (symbol, timestamp);

-- Аналогичные таблицы metrics_1h, metrics_4h, metrics_1d создаются с теми же колонками.
