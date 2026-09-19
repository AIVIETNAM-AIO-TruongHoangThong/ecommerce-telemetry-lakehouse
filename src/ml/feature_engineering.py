"""
ml/feature_engineering.py
--------------------------
TODO: Build the Gold-layer session-level feature table from Silver data.

Features to aggregate per computed_session_id:
  - session_duration_sec   (max_ts - min_ts)
  - total_events
  - view_count, cart_count, remove_count
  - distinct_products, distinct_categories
  - avg_price_viewed, max_price_viewed

Label:
  - is_purchased  (1 if any 'purchase' event in session, else 0)
"""


def build_session_features(df):
    """
    TODO: Group by computed_session_id and aggregate the features above.
    Returns a DataFrame ready for ML training.
    """
    raise NotImplementedError
