# Airflow Setup — Financial Data Pipeline

Apache Airflow environment for collecting, processing, and sinking Yahoo Finance tick data into Kafka → Spark → Iceberg.

## Architecture

```
Yahoo Finance (yfinance)
    ↓  [Kafka]
Spark Streaming (pystrm)
    ↓  [Iceberg / MinIO]
Parquet files + Polar catalog
```

## Directory Structure

```
airflow_setup/
├── config/
│   ├── airflow.cfg          # Airflow runtime config (CeleryExecutor, PostgreSQL, MinIO logging)
│   └── password.json        # SimpleAuthManager credentials
├── dags/
│   ├── airflowTest.py       # Celery worker connectivity test DAG
│   ├── utils.py             # Helpers (e.g. jobdir change)
│   └── yfTicks.py           # Main DAG: market status check → tick fetch → Spark sink → auto-retrigger
├── jobs/
│   ├── config.yml           # App-level config (logging, Postgres, Kafka, Spark, MinIO, Market symbols)
│   ├── tables.yml           # Table metadata for Info / FastInfo (schema type, partition cols, clustering)
│   └── schemas/
│       ├── avro/
│       │   ├── info.avsc    # Avro schema for Yahoo Finance Info
│       │   └── fastinfo.avsc  # Avro schema for Yahoo Finance FastInfo
│       └── json/
│           └── fastinfo.json  # JSON schema (validation) for FastInfo
```

## Key Components

### DAG: `yfTicks`

Scheduled daily at **02:00 IST**.

| Task | Type | Purpose |
|------|------|---------|
| `mStatus` | `@task.virtualenv` | Checks if today is a valid NSE trading day; waits until market open if run early |
| `Yf_Ticks` | `@task.virtualenv` | Fetches live tick data via `pystrm` (isolated venv with `pystrm` dep) |
| `Yf_Spark_Sink` | `@task.virtualenv` | Sinks data to Iceberg via `mynk_etl` (isolated venv with `mynk_etl` dep) |
| `reRunDag` | `@task` | Triggers a new DAG run if data was fetched (auto-retrigger loop) |

### DAG: `celery_worker_test`

Manual-run DAG with 6 PythonOperator tasks that log the worker hostname. Used to verify Celery executor connectivity.

### Config: `jobs/config.yml`

Environment-specific settings under `DEV:`:

- **Logging** — Console + Postgres handlers, JSON formatter
- **PostgreSQL** — Connection to `pgbouncer.bdc.home` for audit/error logs
- **Polaris** — Iceberg catalog REST endpoint
- **MinIO** — Object store for Parquet files
- **Kafka** — Broker bootstrap servers, schema registry, compression settings
- **Spark** — Master URL, executor/driver resources (KIP / TEST profiles)
- **Market** — NSE symbols tracked (RELIANCE, TCS, HDFCBANK, etc.)
- **LOG-CTRL** — Table DDLs for `audit_logs`, `error_logs`, `logs`, `control_tbl`, `tbl_param`, `dq_checks`, `column_dt_map`

### Config: `config/airflow.cfg`

- **Executor:** `CeleryExecutor`
- **Metadata DB:** PostgreSQL (`postgresql+psycopg2://...@postgresql.bdc.home/bdc`)
- **Remote Logging:** MinIO (S3-compatible)
- **Auth:** SimpleAuthManager (`admin:admin`)
- **Timezone:** `Asia/Kolkata`
- **Parallelism:** 32 slots, 16 concurrent tasks per DAG

## Setup

1. Place `config/airflow.cfg` at `$AIRFLOW_HOME/airflow.cfg`
2. Place `config/password.json` at `$AIRFLOW_HOME/config/password.json`
3. Configure Celery broker (Redis/RabbitMQ) and result backend
4. Initialize the Airflow DB: `airflow db migrate`
5. Create the admin user: `airflow users create --username admin --password abcd1234 --role Admin ...`
6. Start scheduler, worker, and webserver

## Data Tables

| Table | Source | Type | Schema | Partition Cols | Clustered By |
|-------|--------|------|--------|---------------|-------------|
| `info` | Kafka | NonStream | AVRO | `year`, `reporting_month` | `symbol` |
| `fastinfo` | Kafka | Streaming | AVRO | `year`, `reporting_month` | `symbol`, `hour` |

## Dependencies (per-task venvs)

| Task | Package |
|------|---------|
| `mStatus` | `pandas_market_calendars` |
| `Yf_Ticks` | `pystrm` |
| `Yf_Spark_Sink` | `mynk_etl` |
