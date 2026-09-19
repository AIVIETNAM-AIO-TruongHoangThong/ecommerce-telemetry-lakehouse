-- tests/assert_valid_session_duration.sql
-- [Custom dbt Test]
--
-- Fails if any session has a negative duration (data quality guard).
-- dbt expects this query to return 0 rows when the test passes.
--
-- TODO [Feature Store]:
--   1. Select rows from mart_ml_features where session_duration_sec < 0.
--   2. Optionally add a tolerance threshold (e.g. < -60 for minor clock skew).

select
    computed_session_id,
    session_duration_sec
from {{ ref('mart_ml_features') }}
where session_duration_sec < 0
-- TODO: implement the assertion body above
