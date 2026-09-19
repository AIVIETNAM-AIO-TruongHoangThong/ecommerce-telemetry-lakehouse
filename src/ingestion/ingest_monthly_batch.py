"""
Module: Monthly Batch Ingestion Engine
Batch Processing: PySpark Ingestion

Key Responsibilities:
1. Accept a --month argument (e.g. 2019-Oct) to process one file at a time.
2. Enforce the explicit REES46 schema (no inferSchema).
3. Read CSV in PERMISSIVE mode, routing corrupt records to the DLQ.
4. Apply basic data quality rules (non-null user_id, non-negative price).
5. Partition and write clean records to Bronze as Snappy Parquet.
"""

import argparse
from pyspark.sql import DataFrame, SparkSession


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the monthly ingestion job."""
    parser = argparse.ArgumentParser(description="PySpark Monthly Batch Ingestion")
    parser.add_argument(
        "--source-url",
        type=str,
        default=None,
        help="HTTP/S3 URL or path to the raw data file (e.g. https://.../2019-Oct.csv.gz).",
    )
    parser.add_argument(
        "--month",
        type=str,
        default="2019-Oct",
        help="Month identifier, e.g. '2019-Oct'.",
    )
    parser.add_argument(
        "--bronze-dir",
        type=str,
        default="data/bronze",
        help="Output directory for Bronze Parquet.",
    )
    parser.add_argument(
        "--dlq-dir",
        type=str,
        default="data/dlq",
        help="Output directory for Dead Letter Queue records.",
    )
    return parser.parse_args()


def read_raw_csv(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read a raw CSV file with the explicit REES46 schema in PERMISSIVE mode.

    TODO [Batch Ingestion]:
    1. Import RAW_EVENT_SCHEMA from schemas.py.
    2. Use spark.read.csv() with schema=RAW_EVENT_SCHEMA, mode='PERMISSIVE',
       columnNameOfCorruptRecord='_corrupt_record'.
    3. Return the raw DataFrame including the _corrupt_record column.
    """
    raise NotImplementedError("Implement read_raw_csv following the docstring.")


def route_to_dlq(df: DataFrame, dlq_path: str) -> DataFrame:
    """
    Isolate and persist bad records; return only the clean subset.

    TODO [Batch Ingestion]:
    1. Filter rows where _corrupt_record IS NOT NULL
       OR user_id IS NULL OR price < 0  -> write to dlq_path as Parquet.
    2. Filter the inverse -> return as the clean DataFrame (drop _corrupt_record).
    """
    raise NotImplementedError("Implement route_to_dlq following the docstring.")


def enrich_timestamps(df: DataFrame) -> DataFrame:
    """
    Cast event_time to TimestampType and extract partition columns.

    TODO [Batch Ingestion]:
    1. Cast event_time STRING -> TimestampType as 'event_timestamp'.
    2. Extract event_year, event_month, event_day using year(), month(), dayofmonth().
    3. Drop the original event_time column.
    """
    raise NotImplementedError("Implement enrich_timestamps following the docstring.")


def write_bronze(df: DataFrame, bronze_path: str) -> None:
    """
    Persist clean DataFrame to Bronze as Snappy-compressed Parquet.

    TODO [Batch Ingestion]:
    1. Use df.write.partitionBy('event_year', 'event_month', 'event_day').
    2. Set compression to 'snappy', format to 'parquet', mode to 'append'.
    """
    raise NotImplementedError("Implement write_bronze following the docstring.")


def main() -> None:
    args = parse_args()
    # TODO: Initialize SparkSession via src.common.spark_session.get_spark_session()
    # TODO: Build input_path from args.input_dir and args.month
    # TODO: Call read_raw_csv() -> route_to_dlq() -> enrich_timestamps() -> write_bronze()
    pass


if __name__ == "__main__":
    main()
