"""
Shared Spark session configuration for MinIO (S3-compatible) and Delta Lake.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping, Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

DEFAULT_MINIO_ENDPOINT = "minio:9000"


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.environ.get(name, default)
    return val if val not in ("", None) else default


def get_spark_session(
    app_name: str,
    *,
    extra_configs: Optional[Mapping[str, str]] = None,
) -> SparkSession:
    """
    Build a SparkSession with Delta Lake extensions and S3A (MinIO) settings.

    Environment variables:
      MINIO_ENDPOINT   — host:port (default: minio:9000)
      MINIO_ACCESS_KEY — S3 access key
      MINIO_SECRET_KEY — S3 secret key
    """
    endpoint = _env("MINIO_ENDPOINT", DEFAULT_MINIO_ENDPOINT) or DEFAULT_MINIO_ENDPOINT
    access_key = _env("MINIO_ACCESS_KEY")
    secret_key = _env("MINIO_SECRET_KEY")

    if not access_key or not secret_key:
        logger.warning("MINIO_ACCESS_KEY or MINIO_SECRET_KEY not set; S3A operations may fail.")

    # Use http for typical MinIO dev setups; path-style required for MinIO
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    if access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
    if secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)

    # Helpful defaults for Delta / S3A
    builder = (
        builder.config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
    )

    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created: app=%s, s3a endpoint=%s", app_name, endpoint)
    return spark


def merge_extra_configs(**kwargs: Any) -> dict[str, str]:
    """Helper to build extra_configs dict with string values only."""
    return {k: str(v) for k, v in kwargs.items() if v is not None}
