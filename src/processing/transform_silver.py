"""
processing/transform_silver.py
-------------------------------
TODO: Transform sessionized Silver data into Star Schema tables
      (dim_products, dim_users, fact_events, fact_sessions).

TODOs:
  1. Load Silver Parquet.
  2. Derive DIM_PRODUCTS: deduplicate by product_id, extract category_l1/l2 from category_code.
  3. Derive DIM_USERS: aggregate first/last seen timestamps, total sessions per user.
  4. Build FACT_EVENTS: select relevant columns with FK references.
  5. Build FACT_SESSIONS: aggregate per computed_session_id (counts, duration, is_purchased).
  6. Return or write each DataFrame.
"""


def build_dim_products(df):
    # TODO
    raise NotImplementedError


def build_dim_users(df):
    # TODO
    raise NotImplementedError


def build_fact_events(df):
    # TODO
    raise NotImplementedError


def build_fact_sessions(df):
    # TODO
    raise NotImplementedError
