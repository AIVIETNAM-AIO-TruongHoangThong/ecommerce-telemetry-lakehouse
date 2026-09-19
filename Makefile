.PHONY: build up down \
        tf-init tf-plan tf-apply tf-destroy \
        ingest sessionize load-snowflake \
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

# --- Infrastructure: Terraform (Snowflake IaC) ---------------------
tf-init:
	cd terraform && terraform init

tf-plan:
	cd terraform && terraform plan

tf-apply:
	cd terraform && terraform apply -auto-approve

tf-destroy:
	cd terraform && terraform destroy -auto-approve

# --- Ingestion: Batch Ingestion (parameterised by MONTH=YYYY-Mon) ---
# Usage: make ingest MONTH=2019-Oct
MONTH ?= 2019-Oct

ingest:
	docker compose exec job-runner spark-submit \
		--master spark://spark-master:7077 \
		src/ingestion/ingest_monthly_batch.py \
		--month $(MONTH) \
		--bronze-dir data/bronze \
		--dlq-dir data/dlq

# Ingest all 7 months sequentially
ingest-all:
	for m in 2019-Oct 2019-Nov 2019-Dec 2020-Jan 2020-Feb 2020-Mar 2020-Apr; do \
		$(MAKE) ingest MONTH=$$m; \
	done

# --- Processing: Sessionization --------------------------------------
sessionize:
	docker compose exec job-runner spark-submit \
		--master spark://spark-master:7077 \
		src/processing/sessionizer.py \
		--input-path data/bronze/$(MONTH)/ \
		--output-path data/silver/$(MONTH)/

# --- Warehouse: Snowflake Load --------------------------------------
load-snowflake:
	docker compose exec job-runner spark-submit \
		--master spark://spark-master:7077 \
		src/warehouse/snowflake_loader.py \
		--source-path data/silver/$(MONTH)/ \
		--target-table SILVER.FACT_EVENTS

# --- Transformation: dbt --------------------------------------------
dbt-deps:
	docker compose exec job-runner dbt deps --project-dir dbt_ecommerce

dbt-run:
	docker compose exec job-runner dbt run --project-dir dbt_ecommerce

dbt-test:
	docker compose exec job-runner dbt test --project-dir dbt_ecommerce

dbt-docs:
	docker compose exec job-runner dbt docs generate --project-dir dbt_ecommerce && \
	docker compose exec job-runner dbt docs serve --project-dir dbt_ecommerce --port 8081

# --- ML Extension: Training & Evaluation ------------------------------------
train-ml:
	docker compose exec job-runner spark-submit \
		--master spark://spark-master:7077 \
		src/ml/train.py \
		--input-path data/artifacts/gold_features/ \
		--model-output data/artifacts/gbt_conversion/

eval-oot:
	docker compose exec job-runner spark-submit \
		--master spark://spark-master:7077 \
		src/ml/evaluate_oot.py \
		--model-path data/artifacts/gbt_conversion/ \
		--test-input data/artifacts/gold_features_oot/ \
		--metrics-output data/artifacts/oot_evaluation.json

# --- Testing -----------------------------------------------------------------
test:
	docker compose exec job-runner pytest tests/ -v --cov=src

# --- Full Pipeline ------------------------------------------------------------
pipeline: ingest-all sessionize load-snowflake dbt-run train-ml eval-oot
