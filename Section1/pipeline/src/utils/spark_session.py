import os
from typing import Optional

from pyspark.sql import SparkSession


def is_glue_environment() -> bool:
    """Check if running in AWS Glue environment."""
    return (
        os.environ.get("AWS_EXECUTION_ENV") is not None
        or "GLUE" in os.environ.get("AWS_EXECUTION_ENV", "").upper()
        or os.path.exists("/opt/amazon")
    )


def create_spark_session(
    app_name: str = "tax-pipeline", master: Optional[str] = None
) -> SparkSession:
    """
    Create Spark session with S3 support for AWS Glue.
    In Glue, master should be None to use Glue's Spark session.
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Only set master if provided (for local development)
    # In Glue, don't set master - use Glue's default
    if master:
        builder = builder.master(master)
    
    builder = (
        builder
        .config("spark.sql.warehouse.dir", "./warehouse")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.debug.maxToStringFields", "200")
    )
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
