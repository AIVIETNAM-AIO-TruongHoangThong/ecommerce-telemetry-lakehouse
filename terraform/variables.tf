# -----------------------------------------------------------------------------
# terraform/variables.tf
# [Infrastructure as Code]
# -----------------------------------------------------------------------------

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. xy12345.us-east-1)"
  type        = string
  sensitive   = true
}

variable "snowflake_username" {
  description = "Snowflake login username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake login password"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role to use for provisioning (e.g. SYSADMIN)"
  type        = string
  default     = "SYSADMIN"
}

variable "warehouse_size" {
  description = "Snowflake virtual warehouse size"
  type        = string
  default     = "X-SMALL"
}
