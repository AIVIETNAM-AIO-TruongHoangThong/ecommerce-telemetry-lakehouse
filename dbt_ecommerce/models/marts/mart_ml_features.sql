-- models/marts/mart_ml_features.sql
-- [Analytics Engineering: ML Feature Store]
-- Materialised as: INCREMENTAL (Gold layer, feeds Spark MLlib training)
--
-- Purpose: Session-level aggregated feature table.
--          This is the Gold layer read by src/ml/train.py and evaluate_oot.py.
--
-- TODO [Feature Store]:
--   1. Reference {{ ref('int_session_metrics') }}.
--   2. Add temporal features: hour_of_day_start, day_of_week_start.
--   3. Add a data_partition column (e.g. '2019-10') for OOT split tracking.
--   4. Add incremental filter on session_start timestamp.
--   5. Ensure is_purchased is cast to BOOLEAN.

{{
    config(
        materialized   = 'incremental',
        unique_key     = 'computed_session_id',
        cluster_by     = ['is_purchased', 'data_partition'],
        on_schema_change = 'append_new_columns'
    )
}}

with session_metrics as (

    select * from {{ ref('int_session_metrics') }}

    {% if is_incremental() %}
    -- TODO [Feature Store]: Add incremental filter on session_start
    {% endif %}

),

feature_store as (

    select
        -- Keys
        null as computed_session_id,
        null as user_id,

        -- Temporal features
        null as session_start,
        null as session_end,
        null as session_duration_sec,
        null as hour_of_day_start,      -- TODO: HOUR(session_start)
        null as day_of_week_start,      -- TODO: DAYOFWEEK(session_start)

        -- Engagement features
        null as total_events,
        null as view_count,
        null as cart_count,
        null as remove_count,
        null as distinct_products,
        null as distinct_categories,

        -- Price features
        null as avg_price_viewed,
        null as max_price_viewed,

        -- Target label
        null as is_purchased,           -- TODO: cast to BOOLEAN

        -- Metadata
        null as data_partition          -- TODO: FORMAT(session_start, 'YYYY-MM')

    from session_metrics

)

select * from feature_store
