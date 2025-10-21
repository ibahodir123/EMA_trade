# Ingestion Service

## Overview
Пакет отвечает за загрузку исторических и потоковых OHLCV‑данных с бирж Binance, MEXC, BingX и подготовку их к дальнейшему расчёту метрик.

## Структура

- `historical_loader.py` — асинхронная выгрузка исторических свечей, gap-check и запись в Parquet/MinIO.
- `stream_listener.py` — потоковый сбор свежих OHLCV с WebSocket/REST fallback и агрегацией по таймфреймам.
- `parquet_writer.py` — Kafka consumer, который формирует Parquet‑файлы и записывает их в MinIO/S3.
- `utils/` — вспомогательные модули (конфигурация, Kafka/MinIO клиенты, gap-check, таймфреймы).

## Статус

| Компонент            | Статус | Комментарий                     |
| -------------------- | ------ | -------------------------------- |
| Конфигурация         | TODO   | Определить переменные среды / Secrets. |
| Исторический загрузчик | TODO | Реализовать выгрузку, gap‑check и запись. |
| Потоковый listener   | TODO   | Реализовать WebSocket/REST fallback. |
| Kafka → Parquet      | TODO   | Реализовать batched writer и дедупликацию. |
