"""
ingestion/ingest_batch.py
-------------------------
Batch Ingestion:
- Local File: Direct Spark read -> DLQ validation -> Iceberg table append.
- Remote URL: Download to scratch -> Spark read -> DLQ validation -> Iceberg table append -> Cleanup.
"""

import os
import shutil
import tempfile
import urllib.request
from typing import Tuple

import click
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from src.common.logger import get_logger
from src.common.spark_session import get_spark_session
from src.ingestion.schemas import RAW_EVENT_SCHEMA

logger = get_logger(__name__)


def download_file_ephemeral(url: str, target_dir: str) -> str:
    """
    Download a remote file into a temporary scratch directory.
    """
    filename = os.path.basename(url)
    local_path = os.path.join(target_dir, filename)
    logger.info(f"Downloading {url} -> {local_path}")
    urllib.request.urlretrieve(url, local_path)
    return local_path


def resolve_source(source: str, target_dir: str) -> Tuple[str, bool]:
    """
    Resolve data source path.
    - If remote URL: download to temporary directory (marked for cleanup).
    - If local file path: use existing file directly.
    Returns: (file_path, is_ephemeral)
    """
    if source.startswith(("http://", "https://")):
        local_path = download_file_ephemeral(source, target_dir)
        return local_path, True

    cleaned_path = source[7:] if source.startswith("file://") else source
    if os.path.exists(cleaned_path):
        logger.info(f"Using local file source: {cleaned_path}")
        return os.path.abspath(cleaned_path), False

    raise FileNotFoundError(
        f"Source '{source}' is neither a reachable remote URL nor an existing local file."
    )


def read_raw_stream(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Read raw CSV.GZ using explicit schema in PERMISSIVE mode.
    """
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_EVENT_SCHEMA)
        .load(file_path)
    )


def split_clean_and_dlq(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Split records into clean dataset and Dead Letter Queue (DLQ).
    Criteria for corrupt:
      - _corrupt_record IS NOT NULL
      - user_id IS NULL
      - price < 0
    """
    is_corrupt = (
        F.col("_corrupt_record").isNotNull()
        | F.col("user_id").isNull()
        | (F.col("price") < 0)
    )

    dlq_df = df.filter(is_corrupt)
    clean_df = df.filter(~is_corrupt).drop("_corrupt_record")

    return clean_df, dlq_df


def enrich_and_repartition(df: DataFrame, num_partitions: int = 8) -> DataFrame:
    """
    Parse event timestamps and repartition for balanced task execution.
    - Format: 'yyyy-MM-dd HH:mm:ss z' -> TimestampType
    - Repartition: 8 partitions across 4 worker cores.
    """
    enriched_df = df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss z"),
    ).drop("event_time")

    return enriched_df.repartition(num_partitions)


def ensure_iceberg_table_exists(
    spark: SparkSession, table_name: str, df: DataFrame
) -> str:
    """
    Create target Iceberg table partitioned by days(event_timestamp) if absent.
    Returns the table identifier.
    """
    catalog_table = table_name if "." in table_name else f"lakehouse.{table_name}"

    if not spark.catalog.tableExists(catalog_table):
        logger.info(f"Table {catalog_table} does not exist. Creating schema...")
        (
            df.limit(0)
            .writeTo(catalog_table)
            .tableProperty("format-version", "2")
            .partitionedBy(F.days("event_timestamp"))
            .create()
        )
    return catalog_table


def write_to_iceberg(df: DataFrame, table_name: str) -> None:
    """
    Append clean records into target Iceberg table.
    """
    catalog_table = table_name if "." in table_name else f"lakehouse.{table_name}"
    logger.info(f"Writing records to Iceberg table: {catalog_table}")
    df.writeTo(catalog_table).append()


def write_to_dlq(df: DataFrame, dlq_dir: str, batch_tag: str) -> None:
    """
    Write quarantined malformed records to MinIO S3A DLQ as Snappy Parquet.
    """
    dlq_target = f"{dlq_dir.rstrip('/')}/batch={batch_tag}"
    logger.warning(f"Routing malformed records to DLQ: {dlq_target}")
    (
        df.write.format("parquet")
        .option("compression", "snappy")
        .mode("append")
        .save(dlq_target)
    )


@click.command(help="Batch Ingestion Engine for Lakehouse Events.")
@click.option(
    "-s",
    "--source",
    "--url",
    "sources",
    multiple=True,
    required=True,
    help="Remote URL(s) or local file path(s) to .csv.gz files. Can be passed multiple times.",
)
@click.option(
    "--table-name",
    default="lakehouse.bronze_events",
    show_default=True,
    help="Target Iceberg table identifier.",
)
@click.option(
    "--warehouse-path",
    default="s3a://ecommerce-lakehouse/iceberg",
    show_default=True,
    help="MinIO S3A Iceberg warehouse root.",
)
@click.option(
    "--dlq-dir",
    default="s3a://ecommerce-lakehouse/dlq",
    show_default=True,
    help="MinIO S3A Dead Letter Queue directory.",
)
@click.option(
    "--enable-dlq",
    is_flag=True,
    default=False,
    help="Quarantine invalid records to DLQ directory (requires extra pass).",
)
def run(
    sources: tuple, table_name: str, warehouse_path: str, dlq_dir: str, enable_dlq: bool
) -> None:
    """
    Batch Ingestion Entry Point.
    """
    logger.info(f"Starting ingestion for {len(sources)} source(s)")
    spark = get_spark_session(warehouse_path=warehouse_path)

    try:
        for source in sources:
            batch_tag = os.path.basename(source).replace(".", "_")
            scratch_dir = tempfile.mkdtemp(prefix="spark_scratch_")

            try:
                # 1. Resolve source (ephemeral download or local path)
                local_file, is_ephemeral = resolve_source(source, scratch_dir)

                # 2. Read raw CSV with static schema
                raw_df = read_raw_stream(spark, local_file)

                # 3. Separate clean vs corrupt
                clean_df, dlq_df = split_clean_and_dlq(raw_df)

                # 4. Optional DLQ routing
                if enable_dlq:
                    write_to_dlq(dlq_df, dlq_dir, batch_tag)

                # 5. Enrich timestamps and repartition
                prepared_df = enrich_and_repartition(clean_df)

                # 6. Ensure Iceberg table exists
                catalog_table = ensure_iceberg_table_exists(
                    spark, table_name, prepared_df
                )

                # 7. Append clean data to Iceberg in single streaming pass
                write_to_iceberg(prepared_df, catalog_table)
                logger.info(f"Successfully processed batch: {source}")

            finally:
                # Clean up temporary scratch directory if downloaded from URL
                if is_ephemeral and os.path.exists(scratch_dir):
                    logger.info(f"Cleaning up temporary directory: {scratch_dir}")
                    shutil.rmtree(scratch_dir)
                elif os.path.exists(scratch_dir) and not os.listdir(scratch_dir):
                    shutil.rmtree(scratch_dir, ignore_errors=True)

    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    run()
