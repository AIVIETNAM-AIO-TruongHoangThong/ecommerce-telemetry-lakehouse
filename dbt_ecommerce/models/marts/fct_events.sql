-- models/marts/fct_events.sql
-- [Analytics Engineering: Mart Layer]
-- Materialised as: INCREMENTAL, clustered by (event_date, event_type)
--
-- Purpose: Fact table for individual e-commerce events.
--          Supports analytical queries on event frequency, funnel analysis,
--          and time-series rollups.
--
-- TODO [Fact Events]:
--   1. Select all enriched columns from {{ ref('stg_raw_events') }}.
--   2. Add event_date = DATE(event_timestamp) for partitioning/clustering.
--   3. Add category_l1, category_l2 by splitting category_code on '.'.
--   4. Implement the is_incremental() filter:
--        WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
--   5. Set unique_key to event_id.

{{
    config(
        materialized   = 'incremental',
        unique_key     = 'event_id',
        cluster_by     = ['event_date', 'event_type'],
        on_schema_change = 'append_new_columns'
    )
}}

with source as (

    select * from {{ ref('stg_raw_events') }}

    {% if is_incremental() %}
    -- TODO [Fact Events]: Add incremental filter here
    {% endif %}

),

enriched as (

    select
        -- TODO: select and derive all required columns
        null as event_id,
        null as event_timestamp,
        null as event_date,         -- derive from event_timestamp
        null as event_type,
        null as product_id,
        null as category_l1,        -- first segment of category_code
        null as category_l2,        -- second segment of category_code
        null as brand,
        null as price,
        null as user_id,
        null as user_session,
        null as computed_session_id -- joined from Silver sessionized data

    from source

)

select * from enriched
