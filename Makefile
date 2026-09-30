.PHONY: build up down \
        download-all download-remainders download-month \
        ingest ingest-all sessionize \
        dbt-deps dbt-run dbt-test dbt-docs \
        train-ml eval-oot \
        test lint

# --- Docker Infrastructure ---------------------------------------------------
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

# --- Fast Multi-Threaded Dataset Downloader ----------------------------------
# Usage: make download-remainders
#        make download-month MONTH=2019-Dec
MONTH ?= 2019-Dec

download-all:
	uv run python scripts/fast_download.py --all --output-dir ./data/scratch

download-remainders:
	uv run python scripts/fast_download.py --all-remainders --output-dir ./data/scratch

download-month:
	uv run python scripts/fast_download.py --url https://data.rees46.com/datasets/marketplace/$(MONTH).csv.gz --output-dir ./data/scratch

# --- Ingestion: Batch Ingestion to MinIO Iceberg (parameterised by MONTH) -----
# Usage: make ingest MONTH=2019-Dec
ingest:
	uv run python -m src.ingestion.ingest_batch \
		--source data/scratch/$(MONTH).csv.gz \
		--table-name lakehouse.bronze_events

# Ingest all 7 months sequentially from local scratch
ingest-all:
	for m in 2019-Oct 2019-Nov 2019-Dec 2020-Jan 2020-Feb 2020-Mar 2020-Apr; do \
		$(MAKE) ingest MONTH=$$m; \
	done

# --- Processing: Silver Sessionization --------------------------------------
sessionize:
	uv run python -m src.processing.transform_silver

# --- Transformation: dbt -----------------------------------------------------
dbt-deps:
	dbt deps --project-dir dbt_ecommerce

dbt-run:
	dbt run --project-dir dbt_ecommerce

dbt-test:
	dbt test --project-dir dbt_ecommerce

dbt-docs:
	dbt docs generate --project-dir dbt_ecommerce && \
	dbt docs serve --project-dir dbt_ecommerce --port 8081

# --- ML Extension: Training & Evaluation ------------------------------------
train-ml:
	uv run python -m src.ml.train

eval-oot:
	uv run python -m src.ml.evaluate_oot

# --- Testing -----------------------------------------------------------------
test:
	pytest tests/ -v --cov=src

# --- Full End-to-End Pipeline ------------------------------------------------
pipeline: download-all ingest-all sessionize dbt-run train-ml eval-oot
