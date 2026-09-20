from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RAW_EVENT_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("product_id", IntegerType(), nullable=False),
        StructField("category_id", StringType(), nullable=True),
        StructField("category_code", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("price", FloatType(), nullable=True),
        StructField("user_id", IntegerType(), nullable=False),
        StructField("user_session", StringType(), nullable=True),
        StructField("_corrupt_record", StringType(), nullable=True),
    ]
)
