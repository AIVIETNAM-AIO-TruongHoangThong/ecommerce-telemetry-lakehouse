-- models/intermediate/int_session_metrics.sql
-- [Analytics Engineering: Intermediate Layer]
-- Materialised as: EPHEMERAL (compiled inline, no physical table)
--
-- Purpose: Aggregate Silver sessionized events into session-level metrics.
--          These metrics feed both fct_events (mart) and mart_ml_features.
--
-- TODO [Intermediate Metrics]:
--   1. Reference {{ ref('stg_raw_events') }} as the upstream model.
--   2. GROUP BY computed_session_id.
--   3. Compute:
-- - session_start = MIN(event_timestamp)
-- - session_end   = MAX(event_timestamp)
-- - session_duration_sec = DATEDIFF('second', session_start, session_end)
-- - total_events, view_count, cart_count, remove_count, purchase_count
-- - distinct_products = COUNT(DISTINCT product_id)
-- - distinct_categories = COUNT(DISTINCT category_code)
-- - avg_price_viewed = AVG(price) FILTER (WHERE event_type = 'view')
-- - max_price_viewed = MAX(price) FILTER (WHERE event_type = 'view')
-- - is_purchased = CASE WHEN purchase_count > 0 THEN TRUE ELSE FALSE END

{{ config(materialized='ephemeral') }}

with session_events as (

    select * from {{ ref('stg_raw_events') }}

),

aggregated as (

    select
        -- TODO: computed_session_id (joined from Silver sessionized data)
        null as computed_session_id,
        null as user_id,
        -- TODO: aggregate the metrics listed above
        null as session_start,
        null as session_end,
        null as session_duration_sec,
        null as total_events,
        null as view_count,
        null as cart_count,
        null as remove_count,
        null as purchase_count,
        null as distinct_products,
        null as distinct_categories,
        null as avg_price_viewed,
        null as max_price_viewed,
        null as is_purchased

    from session_events
    -- TODO: GROUP BY computed_session_id, user_id

)

select * from aggregated
