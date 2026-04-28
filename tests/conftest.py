"""Shared fixtures for pipeline tests."""

import os
import shutil

import pytest
from pyspark.sql import SparkSession

from pipeline.spark_session import ICEBERG_VERSION, SPARK_MAJOR, WAREHOUSE_PATH


@pytest.fixture(scope="session")
def spark():
    """Single Spark session shared across all tests."""
    if os.path.exists(WAREHOUSE_PATH):
        shutil.rmtree(WAREHOUSE_PATH)

    jar_coord = (
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MAJOR}_2.12:"
        f"{ICEBERG_VERSION}"
    )

    session = (
        SparkSession.builder
        .appName("pipeline-tests")
        .master("local[2]")
        .config("spark.jars.packages", jar_coord)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield session
    session.stop()
    if os.path.exists(WAREHOUSE_PATH):
        shutil.rmtree(WAREHOUSE_PATH)
