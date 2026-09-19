# -----------------------------------------------------------------------------
# terraform/outputs.tf
# [Infrastructure as Code]
# -----------------------------------------------------------------------------

output "database_name" {
  description = "The provisioned Snowflake database name"
  value       = snowflake_database.ecommerce_db.name
}

output "warehouse_name" {
  description = "The provisioned Snowflake warehouse name"
  value       = snowflake_warehouse.compute_wh.name
}

output "spark_stage_url" {
  description = "The internal stage URL for Spark bulk loads"
  value       = "@${snowflake_database.ecommerce_db.name}.${snowflake_schema.silver.name}.${snowflake_stage.spark_stage.name}"
}
