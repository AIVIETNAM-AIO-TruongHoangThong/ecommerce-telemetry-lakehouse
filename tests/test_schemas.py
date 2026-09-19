"""
tests/test_schemas.py
---------------------
TODO: Unit tests for the RAW_EVENT_SCHEMA definition.

Suggested test cases:
 - Schema has exactly 9 fields.
 - Required fields (event_time, event_type, product_id, user_id) are non-nullable.
 - Optional fields (category_code, brand, price, category_id, user_session) are nullable.
 - Field types match the spec (e.g. price -> FloatType, user_id -> IntegerType).
"""

import pytest


class TestRawEventSchema:
    def test_field_count(self):
        # TODO
        pass

    def test_required_fields_not_nullable(self):
        # TODO
        pass

    def test_optional_fields_nullable(self):
        # TODO
        pass

    def test_field_types(self):
        # TODO
        pass
