-- =============================================================================
-- gold_feature_store.sql
-- Creates the Gold layer session-level Feature Store table for ML consumption.
-- =============================================================================

USE DATABASE ECOMMERCE_DB;
USE SCHEMA GOLD;

-- -- Gold: Session Feature Store -----------------------------------------------
CREATE TABLE IF NOT EXISTS SESSION_FEATURES (
    computed_session_id     STRING  NOT NULL PRIMARY KEY,
    user_id                 INTEGER NOT NULL,

    -- Temporal features
    session_duration_sec    FLOAT,
    hour_of_day_start       INTEGER,
    day_of_week_start       INTEGER,

    -- Engagement features
    total_events            INTEGER,
    view_count              INTEGER,
    cart_count              INTEGER,
    remove_count            INTEGER,
    distinct_products       INTEGER,
    distinct_categories     INTEGER,

    -- Price features
    avg_price_viewed        FLOAT,
    max_price_viewed        FLOAT,
    total_cart_value        FLOAT,

    -- Target label
    is_purchased            BOOLEAN NOT NULL,

    -- Metadata
    data_partition          STRING  COMMENT 'Source month, e.g. 2019-10',
    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (is_purchased, data_partition);

-- -- View: Feature summary statistics (for EDA / monitoring) ------------------
CREATE OR REPLACE VIEW GOLD.VW_FEATURE_STATS AS
SELECT
    data_partition,
    COUNT(*)                            AS total_sessions,
    SUM(is_purchased::INTEGER)          AS purchased_sessions,
    AVG(is_purchased::FLOAT)            AS conversion_rate,
    AVG(session_duration_sec)           AS avg_session_duration_sec,
    AVG(total_events)                   AS avg_events_per_session,
    AVG(avg_price_viewed)               AS avg_price_viewed
FROM GOLD.SESSION_FEATURES
GROUP BY data_partition
ORDER BY data_partition;
