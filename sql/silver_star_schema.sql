-- =============================================================================
-- silver_star_schema.sql
-- Creates the Silver layer Star Schema: fact and dimension tables.
-- =============================================================================

USE DATABASE ECOMMERCE_DB;
USE SCHEMA SILVER;

-- -- Dimension: Products -------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIM_PRODUCTS (
    product_id          INTEGER       NOT NULL PRIMARY KEY,
    category_id         STRING,
    category_code       STRING,
    category_l1         STRING  COMMENT 'Top-level category (e.g. electronics)',
    category_l2         STRING  COMMENT 'Sub-category (e.g. smartphone)',
    brand               STRING,
    avg_price           FLOAT,
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -- Dimension: Users ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS DIM_USERS (
    user_id             INTEGER       NOT NULL PRIMARY KEY,
    first_seen_at       TIMESTAMP_NTZ,
    last_seen_at        TIMESTAMP_NTZ,
    total_sessions      INTEGER,
    _updated_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -- Fact: Events --------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FACT_EVENTS (
    event_id            STRING        NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    event_timestamp     TIMESTAMP_NTZ NOT NULL,
    event_type          STRING        NOT NULL,
    user_id             INTEGER       NOT NULL REFERENCES DIM_USERS(user_id),
    product_id          INTEGER       REFERENCES DIM_PRODUCTS(product_id),
    user_session        STRING,
    computed_session_id STRING,
    price               FLOAT,
    event_year          INTEGER,
    event_month         INTEGER,
    event_day           INTEGER,
    _ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (event_year, event_month, event_type);

-- -- Fact: Sessions ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FACT_SESSIONS (
    computed_session_id STRING        NOT NULL PRIMARY KEY,
    user_id             INTEGER       NOT NULL REFERENCES DIM_USERS(user_id),
    session_start       TIMESTAMP_NTZ,
    session_end         TIMESTAMP_NTZ,
    session_duration_sec FLOAT,
    total_events        INTEGER,
    view_count          INTEGER,
    cart_count          INTEGER,
    remove_count        INTEGER,
    purchase_count      INTEGER,
    is_purchased        BOOLEAN,
    distinct_products   INTEGER,
    distinct_categories INTEGER,
    avg_price_viewed    FLOAT,
    max_price_viewed    FLOAT,
    _ingested_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (is_purchased);
