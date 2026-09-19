# End-to-End E-Commerce Telemetry Lakehouse & ML Conversion Platform

A fully containerized data engineering and machine learning platform built with PySpark, Snowflake (Medallion Architecture), dbt Core, and Spark MLlib.

---

## 1. Architecture Overview

```
                      +----------------------------------------------------+
                      |    RAW DATA: REES46 CSV Logs (2019-Oct - 2020-Apr) |
                      |                 Mounted via Docker                 |
                      +-------------------------+--------------------------+
                                                |
   +============================================v============================================+
   | STAGE 1: INGESTION ENGINE (PySpark Standalone Cluster)                                   |
   | - Explicit schema enforcement via StructType (avoid inferSchema).                       |
   | - PERMISSIVE mode: Quarantine corrupt/null records into Dead Letter Queue (DLQ).        |
   | - I/O optimization: Persist Snappy Parquet partitioned by (year/month/day).            |
   +============================================+============================================+
                                                |
   +============================================v============================================+
   | STAGE 2: PROCESSING & DISTRIBUTED SESSIONIZATION                                        |
   | - Window functions: lag(event_timestamp), unix_timestamp.                               |
   | - Session inactivity timeout threshold: 1800 seconds (30 minutes).                      |
   | - Assign computed_session_id = hash(user_id || session_index).                          |
   +============================================+============================================+
                                                |
   +============================================v============================================+
   | STAGE 3: MEDALLION DATA WAREHOUSE & ANALYTICS (Snowflake + dbt)                         |
   | - Snowflake Connector for Spark + Internal Staging (SPARK_STAGE).                       |
   | - Bronze: Raw Parquet data loaded into Snowflake staging tables.                        |
   | - Silver: Star Schema (fact_events, fact_sessions, dim_products, dim_users).            |
   | - Gold (Feature Store): Session-level and user-level aggregated metrics via dbt.        |
   +============================================+============================================+
                                                |
   +============================================v============================================+
   | STAGE 4: DISTRIBUTED ML PIPELINE (Spark MLlib)                                          |
   | - Preprocessing: VectorAssembler -> StandardScaler.                                     |
   | - Distributed Training: GBTClassifier on training split.                                |
   | - Out-of-Time Serving: Inference on test split; output ROC-AUC, PR-AUC, Feature Imp.    |
   +=========================================================================================+
```

---

## 2. Repository Layout

```
.
|-- .env.example
|-- .gitignore
|-- Makefile
|-- README.md
|-- pyproject.toml
|-- requirements.txt
|-- docker-compose.yml
|-- docker/
|   |-- spark/
|   |   |-- Dockerfile
|   |   `-- jars/
|   `-- app/
|       `-- Dockerfile
|-- configs/
|   |-- spark_config.yaml
|   |-- snowflake_config.yaml
|   `-- ml_config.yaml
|-- terraform/
|   |-- main.tf
|   |-- variables.tf
|   |-- outputs.tf
|   `-- providers.tf
|-- dbt_ecommerce/
|   |-- dbt_project.yml
|   |-- packages.yml
|   |-- profiles.yml.template
|   |-- models/
|   |   |-- staging/
|   |   |-- intermediate/
|   |   `-- marts/
|   `-- tests/
|-- data/
|   |-- raw/
|   |-- bronze/
|   |-- silver/
|   |-- dlq/
|   |-- gold_features/
|   |-- metrics/
|   `-- artifacts/
|-- sql/
|   |-- snowflake_setup.sql
|   |-- silver_star_schema.sql
|   `-- gold_feature_store.sql
|-- src/
|   `-- ecommerce_behavior_multi_category/
|       |-- common/
|       |   |-- logger.py
|       |   `-- spark_session.py
|       |-- ingestion/
|       |   |-- schemas.py
|       |   `-- ingest_monthly_batch.py
|       |-- processing/
|       |   |-- sessionizer.py
|       |   `-- transform_silver.py
|       |-- warehouse/
|       |   `-- snowflake_loader.py
|       `-- ml/
|           |-- feature_engineering.py
|           |-- train.py
|           `-- evaluate_oot.py
`-- tests/
    |-- conftest.py
    |-- test_schemas.py
    `-- test_sessionizer.py
```

---

## 3. Quickstart & CLI Commands

### Environment Setup
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials and settings
```

### Docker Infrastructure
```bash
make build   # Build Docker images
make up      # Start Spark cluster (master, worker, job-runner)
make down    # Stop containers
```

### Infrastructure as Code (Terraform)
```bash
make tf-init
make tf-plan
make tf-apply
```

### Data Pipeline Execution
```bash
# Stage 1: Batch Ingestion (per month)
make ingest MONTH=2019-Oct

# Stage 2: Sessionization
make sessionize MONTH=2019-Oct

# Stage 3: Load to Snowflake
make load-snowflake MONTH=2019-Oct

# Stage 4: dbt Transformations & Testing
make dbt-deps
make dbt-run
make dbt-test

# Stage 5: ML Training & Out-of-Time Evaluation
make train-ml
make eval-oot
```

### Testing
```bash
make test
```
