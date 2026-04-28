"""
Shared Spark session factory. Configures Iceberg catalog and warehouse path.
All pipeline modules import get_spark() rather than building their own session.
"""

import os
from pyspark.sql import SparkSession

ICEBERG_VERSION = "1.7.1"
SPARK_MAJOR = "3.5"

WAREHOUSE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "warehouse",
)


def get_spark(app_name: str = "lakehouse-etl") -> SparkSession:
    jar_coord = (
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_MAJOR}_2.12:"
        f"{ICEBERG_VERSION}"
    )

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", jar_coord)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", WAREHOUSE_PATH)
        # Sensible defaults for local development
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
