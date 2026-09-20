import os
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "ecommerce-lakehouse-ingestion",
    warehouse_path: str = "s3a://ecommerce-lakehouse/iceberg",
    minio_endpoint: str = "http://minio:9000",
) -> SparkSession:
    """
    Build and return a SparkSession with Apache Iceberg and MinIO S3A support.
    """
    # Allow environment variable overrides
    endpoint = os.getenv("MINIO_ENDPOINT", minio_endpoint)
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    # Spark packages for Iceberg and Hadoop AWS S3A
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        # JVM Driver Memory & Garbage Collection Tuning
        .config("spark.driver.memory", "4g")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.2")
        # Pull Maven artifacts at runtime
        .config("spark.jars.packages", ",".join(packages))
        # Apache Iceberg SQL Extensions & Catalog Setup
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.warehouse", warehouse_path)
        # MinIO S3A Filesystem Configuration
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
