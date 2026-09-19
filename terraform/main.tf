# -----------------------------------------------------------------------------
# terraform/main.tf
# [Infrastructure as Code]
# Provisions: ECOMMERCE_DB, BRONZE/SILVER/GOLD schemas,
#             COMPUTE_WH warehouse, and SPARK_STAGE internal stage.
# -----------------------------------------------------------------------------

# -- Database ------------------------------------------------------------------
resource "snowflake_database" "ecommerce_db" {
  name    = "ECOMMERCE_DB"
  comment = "E-Commerce Telemetry Lakehouse (Medallion Architecture)"
}

# -- Schemas -------------------------------------------------------------------
resource "snowflake_schema" "bronze" {
  database = snowflake_database.ecommerce_db.name
  name     = "BRONZE"
  comment  = "Raw Parquet data loaded from PySpark ingestion"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.ecommerce_db.name
  name     = "SILVER"
  comment  = "Star-schema fact and dimension tables"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.ecommerce_db.name
  name     = "GOLD"
  comment  = "Aggregated feature store for ML and BI consumption"
}

# -- Compute Warehouse ---------------------------------------------------------
resource "snowflake_warehouse" "compute_wh" {
  name           = "COMPUTE_WH"
  warehouse_size = var.warehouse_size
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Primary compute warehouse for ecommerce pipeline"
}

# -- Internal Stage (for Spark bulk loads) ------------------------------------
resource "snowflake_stage" "spark_stage" {
  database = snowflake_database.ecommerce_db.name
  schema   = snowflake_schema.silver.name
  name     = "SPARK_STAGE"
  comment  = "Internal stage used by Spark-Snowflake connector for bulk transfers"
}
