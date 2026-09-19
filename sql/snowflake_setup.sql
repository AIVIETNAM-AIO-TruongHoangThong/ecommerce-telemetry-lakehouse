-- =============================================================================
-- snowflake_setup.sql
-- Initializes the ECOMMERCE_DB database with BRONZE, SILVER, and GOLD schemas.
-- Run once as SYSADMIN (or a role with CREATE DATABASE privilege).
-- =============================================================================

USE ROLE SYSADMIN;

-- -- Database ------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;
USE DATABASE ECOMMERCE_DB;

-- -- Schemas -------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS BRONZE COMMENT = 'Raw event data loaded from Parquet';
CREATE SCHEMA IF NOT EXISTS SILVER COMMENT = 'Star-schema fact and dimension tables';
CREATE SCHEMA IF NOT EXISTS GOLD   COMMENT = 'Aggregated feature store for ML';

-- -- Compute Warehouse ---------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND   = 300
    AUTO_RESUME    = TRUE
    COMMENT        = 'Primary compute warehouse for ecommerce pipeline';

-- -- Bronze: Raw staging table -------------------------------------------------
CREATE TABLE IF NOT EXISTS BRONZE.RAW_EVENTS (
    event_time      STRING,
    event_type      STRING,
    product_id      STRING,
    category_id     STRING,
    category_code   STRING,
    brand           STRING,
    price           STRING,
    user_id         STRING,
    user_session    STRING,
    _ingested_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
