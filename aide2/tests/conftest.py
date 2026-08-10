"""Shared pytest fixtures for AIDE2 tests."""

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    A local, in-process SparkSession for unit testing pure transformation
    functions in aide2/spark/*.py. Deliberately has no S3A/MinIO or Delta Lake
    configuration — those are only needed by the I/O-bound run_bronze/run_silver
    /run_gold orchestrators, which are out of scope for these unit tests.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("aide2-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
