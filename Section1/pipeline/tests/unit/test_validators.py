import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.silver.validators import Validator
from src.utils.logger import StructuredLogger


@pytest.fixture
def spark():
    return SparkSession.builder.appName("test").master("local[2]").getOrCreate()


@pytest.fixture
def logger():
    return StructuredLogger("test")


@pytest.fixture
def config():
    return {
        "validation": {
            "nric": {"pattern": "^[STFG]\\d{7}[A-Z]$", "allow_empty": False},
            "postal_code": {
                "pattern": "^\\d{6}$",
                "min_value": 10000,
                "max_value": 829999,
            },
            "tax_calculation": {"tolerance": 0.1},
            "cpf": {"non_resident_must_be_zero": True},
            "date": {
                "iso_pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                "ddmmyyyy_pattern": "^\\d{1,2}/\\d{1,2}/\\d{4}$",
            },
        }
    }


@pytest.fixture
def sample_df(spark):
    schema = StructType(
        [
            StructField("nric", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("filing_date", StringType(), True),
            StructField("assessment_year", IntegerType(), True),
            StructField("annual_income_sgd", DoubleType(), True),
            StructField("chargeable_income_sgd", DoubleType(), True),
            StructField("total_reliefs_sgd", DoubleType(), True),
            StructField("residential_status", StringType(), True),
            StructField("cpf_contributions_sgd", DoubleType(), True),
        ]
    )

    data = [
        (
            "S1234567A",
            "119077",
            "2024-03-15",
            2023,
            85000.0,
            72050.0,
            12950.0,
            "Resident",
            17000.0,
        ),
        (
            "",
            "INVALID",
            "2024-13-45",
            2023,
            75000.0,
            65000.0,
            10000.0,
            "Non-Resident",
            5000.0,
        ),
        (
            "S2345678B",
            "238882",
            "2022-02-28",
            2023,
            95000.0,
            79100.0,
            15900.0,
            "Resident",
            19000.0,
        ),
    ]

    return spark.createDataFrame(data, schema)


def test_validate_nric(spark, logger, config, sample_df):
    validator = Validator(config, logger)
    df, is_valid = validator.validate_nric(sample_df)

    results = df.select("nric", "has_invalid_nric").collect()

    assert results[0]["has_invalid_nric"] == False
    assert results[1]["has_invalid_nric"] == True
    assert results[2]["has_invalid_nric"] == False


def test_validate_postal_code(spark, logger, config, sample_df):
    validator = Validator(config, logger)
    df, is_valid = validator.validate_postal_code(sample_df)

    results = df.select("postal_code", "has_invalid_postal_code").collect()

    assert results[0]["has_invalid_postal_code"] == False
    assert results[1]["has_invalid_postal_code"] == True
    assert results[2]["has_invalid_postal_code"] == False


def test_validate_tax_calculation(spark, logger, config, sample_df):
    validator = Validator(config, logger)
    df, is_valid = validator.validate_tax_calculation(sample_df)

    results = df.select("has_calculation_error").collect()

    assert results[0]["has_calculation_error"] == False
    assert results[1]["has_calculation_error"] == False
    assert results[2]["has_calculation_error"] == False


def test_validate_cpf_rule(spark, logger, config, sample_df):
    validator = Validator(config, logger)
    df, is_valid = validator.validate_cpf_rule(sample_df)

    results = df.select(
        "residential_status", "cpf_contributions_sgd", "has_business_rule_violation"
    ).collect()

    assert results[1]["has_business_rule_violation"] == True


def test_validate_filing_date(spark, logger, config, sample_df):
    validator = Validator(config, logger)
    df, is_valid = validator.validate_filing_date(sample_df)
    
    results = df.select("filing_date", "has_invalid_date", "assessment_year").collect()
    assert results[0]["has_invalid_date"] == False
    assert results[1]["has_invalid_date"] == False
    assert results[2]["has_invalid_date"] == True


def test_validate_filing_date_ddmmyyyy(spark, logger, config):
    schema = StructType([
        StructField("filing_date", StringType(), True),
        StructField("assessment_year", IntegerType(), True)
    ])
    data = [("15/03/2024", 2023), ("15/03/2022", 2023)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config, logger)
    df_result, is_valid = validator.validate_filing_date(df)
    
    rows = df_result.collect()
    assert rows[0]["has_invalid_date"] == False
    assert rows[1]["has_invalid_date"] == True


def test_validate_completeness(spark, logger, config, sample_df):
    validator = Validator(config, logger)
    df, is_valid = validator.validate_completeness(sample_df)
    
    assert "has_completeness_issue" in df.columns


def test_validate_nric_allow_empty(spark, logger):
    config_allow_empty = {
        "validation": {
            "nric": {"pattern": "^[STFG]\\d{7}[A-Z]$", "allow_empty": True}
        }
    }
    schema = StructType([StructField("nric", StringType(), True)])
    data = [("",), ("S1234567A",)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config_allow_empty, logger)
    df_result, is_valid = validator.validate_nric(df)
    
    rows = df_result.collect()
    assert rows[0]["has_invalid_nric"] == False
    assert rows[1]["has_invalid_nric"] == False


def test_apply_all_validations(spark, logger, config, sample_df):
    validator = Validator(config, logger)
    result_df = validator.apply_all_validations(sample_df)
    
    assert "_nric_score" in result_df.columns
    assert "_postal_score" in result_df.columns
    assert "_date_score" in result_df.columns
    assert "_calc_score" in result_df.columns
    assert "_cpf_score" in result_df.columns
    assert "_completeness_score" in result_df.columns


def test_validate_postal_code_edge_cases(spark, logger, config):
    schema = StructType([StructField("postal_code", StringType(), True)])
    data = [("99999",), ("830000",), ("100000",), ("829999",)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config, logger)
    df_result, is_valid = validator.validate_postal_code(df)
    
    rows = df_result.collect()
    assert rows[0]["has_invalid_postal_code"] == True
    assert rows[1]["has_invalid_postal_code"] == True
    assert rows[2]["has_invalid_postal_code"] == False
    assert rows[3]["has_invalid_postal_code"] == False


def test_validate_postal_code_null(spark, logger, config):
    schema = StructType([StructField("postal_code", StringType(), True)])
    data = [(None,), ("",)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config, logger)
    df_result, is_valid = validator.validate_postal_code(df)
    
    rows = df_result.collect()
    assert rows[0]["has_invalid_postal_code"] == True
    assert rows[1]["has_invalid_postal_code"] == True


def test_validate_tax_calculation_with_error(spark, logger, config):
    schema = StructType([
        StructField("annual_income_sgd", DoubleType(), True),
        StructField("chargeable_income_sgd", DoubleType(), True),
        StructField("total_reliefs_sgd", DoubleType(), True)
    ])
    data = [(85000.0, 70000.0, 12950.0)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config, logger)
    df_result, is_valid = validator.validate_tax_calculation(df)
    
    rows = df_result.collect()
    assert rows[0]["has_calculation_error"] == True


def test_validate_cpf_rule_disabled(spark, logger):
    config_no_cpf = {
        "validation": {
            "cpf": {"non_resident_must_be_zero": False}
        }
    }
    schema = StructType([
        StructField("residential_status", StringType(), True),
        StructField("cpf_contributions_sgd", DoubleType(), True)
    ])
    data = [("Non-Resident", 5000.0)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config_no_cpf, logger)
    df_result, is_valid = validator.validate_cpf_rule(df)
    
    rows = df_result.collect()
    assert rows[0]["has_business_rule_violation"] == False


def test_validate_tax_calculation_null_values(spark, logger, config):
    schema = StructType([
        StructField("annual_income_sgd", DoubleType(), True),
        StructField("chargeable_income_sgd", DoubleType(), True),
        StructField("total_reliefs_sgd", DoubleType(), True)
    ])
    data = [(None, None, None), (85000.0, 72050.0, 12950.0)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config, logger)
    df_result, is_valid = validator.validate_tax_calculation(df)
    
    rows = df_result.collect()
    # Null values should be handled gracefully
    assert len(rows) == 2


def test_validate_filing_date_null(spark, logger, config):
    schema = StructType([
        StructField("filing_date", StringType(), True),
        StructField("assessment_year", IntegerType(), True)
    ])
    data = [(None, 2023), ("2024-03-15", 2023)]
    df = spark.createDataFrame(data, schema)
    
    validator = Validator(config, logger)
    df_result, is_valid = validator.validate_filing_date(df)
    
    rows = df_result.collect()
    assert rows[0]["has_invalid_date"] == True  # None should be invalid
    assert rows[1]["has_invalid_date"] == False


def test_validate_completeness_empty_checks(spark, logger, config):
    # Test with DataFrame that has no columns (empty completeness checks)
    schema = StructType([])
    df = spark.createDataFrame([()], schema)
    
    validator = Validator(config, logger)
    df_result, is_valid = validator.validate_completeness(df)
    
    # Should handle empty checks gracefully
    assert "has_completeness_issue" in df_result.columns
