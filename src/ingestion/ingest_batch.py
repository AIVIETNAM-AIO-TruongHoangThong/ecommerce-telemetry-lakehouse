"""
ingestion/ingest_batch.py
-------------------------
Production Ingestion Engine:
Remote .csv.gz -> Ephemeral Scratch -> Spark Ingestion -> DLQ Filter -> Iceberg Table.
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
    Stream a remote file into the container's ephemeral scratch space.
    """
    filename = os.path.basename(url)
    local_path = os.path.join(target_dir, filename)
    logger.info(f"Downloading {url} -> {local_path}")
    urllib.request.urlretrieve(url, local_path)
    return local_path


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


def enrich_and_repartition(df: DataFrame) -> DataFrame:
    """
    Enrich timestamps and repartition for optimal write parallelism.
    - Format: 'yyyy-MM-dd HH:mm:ss z' -> TimestampType
    - Repartition: 4 partitions to match 2 cores across 2 scheduling rounds.
    """
    enriched_df = df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss z"),
    ).drop("event_time")

    # 4 partitions for 2 cores = 2 balanced waves of task execution
    return enriched_df.repartition(4)


def ensure_iceberg_table_exists(
    spark: SparkSession, table_name: str, df: DataFrame
) -> None:
    """
    Create the target Iceberg table partitioned by days(event_timestamp) if absent.
    """
    catalog_table = table_name if "." in table_name else f"lakehouse.{table_name}"

    if not spark.catalog.tableExists(catalog_table):
        logger.info(f"Table {catalog_table} does not exist. Creating it...")
        (
            df.writeTo(catalog_table)
            .tableProperty("format-version", "2")
            .partitionedBy(F.days("event_timestamp"))
            .create()
        )


def write_to_iceberg(df: DataFrame, table_name: str) -> None:
    """
    Append clean records into the target Iceberg table.
    """
    catalog_table = table_name if "." in table_name else f"lakehouse.{table_name}"
    logger.info(f"Writing records to Iceberg table: {catalog_table}")
    df.writeTo(catalog_table).append()


def write_to_dlq(df: DataFrame, dlq_dir: str, batch_tag: str) -> None:
    """
    Write quarantined malformed records to MinIO S3A DLQ as Snappy Parquet.
    """
    dlq_target = f"{dlq_dir.rstrip('/')}/batch={batch_tag}"
    logger.warn(f"Quarantining corrupt records to DLQ: {dlq_target}")
    (
        df.write.format("parquet")
        .option("compression", "snappy")
        .mode("append")
        .save(dlq_target)
    )


@click.command()
@click.option(
    "--url",
    "urls",
    multiple=True,
    required=True,
    help="Remote URL(s) to .csv.gz files. Can be passed multiple times.",
)
@click.option(
    "--table-name",
    default="lakehouse.bronze_events",
    help="Target Iceberg table identifier.",
)
@click.option(
    "--warehouse-path",
    default="s3a://ecommerce-lakehouse/iceberg",
    help="MinIO S3A Iceberg warehouse root.",
)
@click.option(
    "--dlq-dir",
    default="s3a://ecommerce-lakehouse/dlq",
    help="MinIO S3A Dead Letter Queue directory.",
)
def run(urls: tuple, table_name: str, warehouse_path: str, dlq_dir: str) -> None:
    """
    Batch Ingestion Entry Point.
    """
    logger.info(f"Starting Ingestion Job for {len(urls)} URL(s)")
    spark = get_spark_session(warehouse_path=warehouse_path)

    try:
        for url in urls:
            batch_tag = os.path.basename(url).replace(".", "_")
            scratch_dir = tempfile.mkdtemp(prefix="spark_scratch_")

            try:
                # 1. Ephemeral download (zero host disk footprint)
                local_file = download_file_ephemeral(url, scratch_dir)

                # 2. Read raw CSV with static schema
                raw_df = read_raw_stream(spark, local_file)

                # 3. Separate clean vs corrupt
                clean_df, dlq_df = split_clean_and_dlq(raw_df)

                # 4. Handle DLQ if corrupt records exist
                # Note: dlq_df.isEmpty() is lazily evaluated without full table scan
                if not dlq_df.rdd.isEmpty():
                    write_to_dlq(dlq_df, dlq_dir, batch_tag)

                # 5. Enrich timestamps and repartition
                prepared_df = enrich_and_repartition(clean_df)

                # 6. Ensure Iceberg table exists with hidden partitioning
                ensure_iceberg_table_exists(spark, table_name, prepared_df)

                # 7. Append clean data to Iceberg
                write_to_iceberg(prepared_df, table_name)
                logger.info(f"Successfully processed batch: {url}")

            finally:
                # Zero-Storage Guarantee: Purge scratch directory immediately
                if os.path.exists(scratch_dir):
                    logger.info(f"Purging scratch space: {scratch_dir}")
                    shutil.rmtree(scratch_dir)

    finally:
        spark.stop()
        logger.info("SparkSession stopped cleanly.")


if __name__ == "__main__":
    run()
