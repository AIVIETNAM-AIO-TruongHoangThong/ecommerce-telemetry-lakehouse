"""
tests/test_schemas.py
---------------------
Unit tests for the RAW_EVENT_SCHEMA definition.
"""

from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
)
from src.ingestion.schemas import RAW_EVENT_SCHEMA


class TestRawEventSchema:
    def test_field_count(self):
        # 9 standard event columns + 1 _corrupt_record column for PERMISSIVE mode
        assert len(RAW_EVENT_SCHEMA.fields) == 10

    def test_required_fields_not_nullable(self):
        required_fields = ["event_time", "event_type", "product_id", "user_id"]
        for field_name in required_fields:
            field = RAW_EVENT_SCHEMA[field_name]
            assert field.nullable is False, (
                f"Field '{field_name}' should be non-nullable"
            )

    def test_optional_fields_nullable(self):
        optional_fields = [
            "category_id",
            "category_code",
            "brand",
            "price",
            "user_session",
            "_corrupt_record",
        ]
        for field_name in optional_fields:
            field = RAW_EVENT_SCHEMA[field_name]
            assert field.nullable is True, f"Field '{field_name}' should be nullable"

    def test_field_types(self):
        assert isinstance(RAW_EVENT_SCHEMA["event_time"].dataType, StringType)
        assert isinstance(RAW_EVENT_SCHEMA["event_type"].dataType, StringType)
        assert isinstance(RAW_EVENT_SCHEMA["product_id"].dataType, IntegerType)
        assert isinstance(RAW_EVENT_SCHEMA["price"].dataType, FloatType)
        assert isinstance(RAW_EVENT_SCHEMA["user_id"].dataType, IntegerType)
        assert isinstance(RAW_EVENT_SCHEMA["user_session"].dataType, StringType)
