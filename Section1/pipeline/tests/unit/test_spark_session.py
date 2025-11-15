import pytest
from pyspark.sql import SparkSession

from src.utils.spark_session import create_spark_session


def test_create_spark_session_default():
    spark = create_spark_session()
    try:
        assert isinstance(spark, SparkSession)
        app_name = spark.sparkContext.appName
        assert app_name in ["tax-pipeline", "test"]
    finally:
        spark.stop()


def test_create_spark_session_custom_name():
    spark = create_spark_session("custom-app")
    try:
        assert spark.sparkContext.appName == "custom-app"
    finally:
        spark.stop()


def test_create_spark_session_custom_master():
    spark = create_spark_session("test", "local[1]")
    try:
        assert spark.sparkContext.master == "local[1]"
    finally:
        spark.stop()


def test_create_spark_session_config():
    spark = create_spark_session("test")
    try:
        conf = spark.sparkContext.getConf()
        assert conf.get("spark.sql.adaptive.enabled") == "true"
        assert conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true"
    finally:
        spark.stop()

