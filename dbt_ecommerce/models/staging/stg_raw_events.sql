-- models/staging/stg_raw_events.sql
-- [Analytics Engineering: Staging Layer]
-- Materialised as: VIEW (read directly from Bronze raw table)
--
-- Purpose: Lightly clean and type-cast raw Bronze event records.
--          No business logic here - only renaming, casting, and nullability.
--
-- TODO [Staging]:
--   1. Cast event_time (STRING) to TIMESTAMP_NTZ.
--   2. Trim and lower-case event_type, brand, category_code.
--   3. Cast product_id, user_id to INTEGER; price to FLOAT.
--   4. Add a surrogate key: {{ dbt_utils.generate_surrogate_key(['user_id', 'event_time', 'event_type']) }}
--   5. Filter out rows where user_id IS NULL or price < 0.

{{ config(materialized='view') }}

with source as (

    select * from {{ source('bronze', 'raw_events') }}

),

renamed as (

    select
        -- TODO: cast and rename columns here
        null as event_id,           -- surrogate key
        null as event_timestamp,    -- cast event_time -> TIMESTAMP_NTZ
        null as event_type,
        null as product_id,
        null as category_id,
        null as category_code,
        null as brand,
        null as price,
        null as user_id,
        null as user_session

    from source

)

select * from renamed
-- TODO: add WHERE clause to exclude nulls/negatives
