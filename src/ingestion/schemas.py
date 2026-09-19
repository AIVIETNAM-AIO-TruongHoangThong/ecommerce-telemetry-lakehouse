"""
ingestion/schemas.py
--------------------
TODO: Define the explicit StructType schema for the REES46 raw event CSV.

Columns:
  event_time, event_type, product_id, category_id,
  category_code, brand, price, user_id, user_session
"""

from pyspark.sql.types import StructType

# TODO: Define RAW_EVENT_SCHEMA using StructType / StructField
RAW_EVENT_SCHEMA: StructType = None  # replace with your definition
