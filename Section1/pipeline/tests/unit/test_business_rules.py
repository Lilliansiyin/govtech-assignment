import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.silver.business_rules import BusinessRules
from src.utils.logger import StructuredLogger


@pytest.fixture
def spark():
    return SparkSession.builder.appName("test").master("local[2]").getOrCreate()


@pytest.fixture
def logger():
    return StructuredLogger("test")


@pytest.fixture
def config():
    return {}


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("residential_status", StringType(), True),
        StructField("cpf_contributions_sgd", DoubleType(), True)
    ])
    data = [
        ("Resident", 17000.0),
        ("Non-Resident", 5000.0),
        ("Non-Resident", 0.0),
        ("Resident", 19000.0)
    ]
    return spark.createDataFrame(data, schema)


def test_business_rules_init(logger, config):
    rules = BusinessRules(config, logger)
    assert rules.config == config
    assert rules.logger == logger


def test_apply_cpf_correction(spark, logger, config, sample_df):
    rules = BusinessRules(config, logger)
    result_df = rules.apply_cpf_correction(sample_df)
    
    rows = result_df.collect()
    assert rows[0]["cpf_contributions_sgd"] == 17000.0
    assert rows[1]["cpf_contributions_sgd"] == 0.0
    assert rows[2]["cpf_contributions_sgd"] == 0.0
    assert rows[3]["cpf_contributions_sgd"] == 19000.0


def test_apply_cpf_correction_no_correction(spark, logger, config):
    schema = StructType([
        StructField("residential_status", StringType(), True),
        StructField("cpf_contributions_sgd", DoubleType(), True)
    ])
    data = [("Resident", 1000.0), ("Non-Resident", 0.0)]
    df = spark.createDataFrame(data, schema)
    
    rules = BusinessRules(config, logger)
    result_df = rules.apply_cpf_correction(df)
    
    rows = result_df.collect()
    assert rows[0]["cpf_contributions_sgd"] == 1000.0
    assert rows[1]["cpf_contributions_sgd"] == 0.0


def test_apply_cpf_correction_with_logging(spark, logger, config):
    # Test that logging happens when corrections are made
    from unittest.mock import patch
    schema = StructType([
        StructField("residential_status", StringType(), True),
        StructField("cpf_contributions_sgd", DoubleType(), True)
    ])
    data = [("Non-Resident", 5000.0), ("Non-Resident", 3000.0)]
    df = spark.createDataFrame(data, schema)
    
    rules = BusinessRules(config, logger)
    with patch.object(logger, 'info') as mock_info:
        result_df = rules.apply_cpf_correction(df)
        # Should log when corrections are made
        mock_info.assert_called()
        call_args = str(mock_info.call_args)
        assert "Auto-corrected CPF" in call_args or "non-resident" in call_args.lower()
    
    rows = result_df.collect()
    assert rows[0]["cpf_contributions_sgd"] == 0.0
    assert rows[1]["cpf_contributions_sgd"] == 0.0

