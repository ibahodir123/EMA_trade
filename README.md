# Uptrend Strategy Repository

## Структура проекта
```
/
+-- config/                 # Конфигурационные файлы (rule engine и производные)
+-- dags/                   # Airflow DAG'и для ingestion и расчёта метрик
+-- docs/                   # Техническая документация и шаблоны отчётов
+-- infrastructure/         # Скрипты/схемы инфраструктуры (ClickHouse, Kafka, etc.)
+-- ingestion/              # DEV-1: сбор данных (исторический и потоковый)
+-- metrics/                # DEV-2: расчёт метрик и загрузка в витрины
+-- rule_engine/            # DEV-3/DEV-4: реализация Rule Engine (stub)
+-- requirements.txt        # Python зависимости
L-- README.md               # Этот файл
```

## Текущие этапы
- **DEV-1 (Data Ingestion)**: реализованы модули исторической загрузки, потокового прослушивания и записи Parquet. Airflow DAG'и `ohlcv_backfill` и `ohlcv_incremental` запускают соответствующие пайплайны.
- **DEV-2 (Metrics Engine)**: реализованы расчёт метрик (EMA + производные), перегон в ClickHouse, инкрементальные DAG'и `metrics_calculation_daily` и `metrics_calculation_incremental`, а также набор тестов.

## Запуск
1. Установите зависимости: `pip install -r requirements.txt`.
2. Настройте переменные окружения (доступ к MinIO/S3, ClickHouse, Vault).
3. Загрузите DAG'и в Airflow и запустите пайплайны `ohlcv_*` и `metrics_*`.

## Тесты
```
pytest metrics/tests
```

## Документация
Основное ТЗ и другие артефакты находятся в папке `docs/`.
