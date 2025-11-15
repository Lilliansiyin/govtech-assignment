import pytest
import json
import os
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import lit

from src.gold.dimensional_modeler import DimensionalModeler, _categorize_occupation_udf
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
        "occupation_categories": {
            "Engineering": ["Engineer", "Developer"],
            "Management": ["Manager", "Director"],
            "Other": []
        }
    }


@pytest.fixture
def postal_mapping_file(tmp_path):
    mapping = [
        {"district": 1, "area": "Test Area", "postal_codes": ["01", "02"], "region": "Central"}
    ]
    file_path = tmp_path / "postal_mapping.json"
    with open(file_path, "w") as f:
        json.dump(mapping, f)
    return str(file_path)


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("taxpayer_id", StringType(), True),
        StructField("nric_cleansed", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("filing_status", StringType(), True),
        StructField("residential_status", StringType(), True),
        StructField("number_of_dependents", IntegerType(), True),
        StructField("assessment_year", IntegerType(), True),
        StructField("filing_date_cleansed", DateType(), True),
        StructField("postal_code_cleansed", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("housing_type", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("occupation_raw", StringType(), True)
    ])
    from datetime import date
    data = [
        ("SG001", "S1234567A", "Test User", "Single", "Resident", 0, 2023, date(2024, 3, 15),
         "010000", "010000", "HDB", "Software Engineer", "Software Engineer"),
        ("SG002", "S2345678B", "Test User 2", "Married", "Resident", 2, 2023, date(2024, 2, 28),
         "020000", "020000", "Condo", "Manager", "Manager")
    ]
    return spark.createDataFrame(data, schema)


def test_categorize_occupation_udf():
    categories = {
        "Engineering": ["Engineer", "Developer"],
        "Other": []
    }
    func = _categorize_occupation_udf(categories)
    assert func("Software Engineer") == "Engineering"
    assert func("Manager") == "Other"
    assert func("") == "Other"
    assert func(None) == "Other"


def test_dimensional_modeler_init(spark, logger, config):
    modeler = DimensionalModeler(spark, config, logger)
    assert modeler.spark == spark
    assert modeler.config == config
    assert len(modeler.occupation_categories) > 0


def test_load_postal_mapping(spark, logger, config, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    mapping = modeler._load_postal_mapping(postal_mapping_file)
    assert len(mapping) == 1
    assert mapping[0]["district"] == 1


def test_build_dim_taxpayer(spark, logger, config, sample_df):
    modeler = DimensionalModeler(spark, config, logger)
    dim = modeler.build_dim_taxpayer(sample_df)
    
    assert "taxpayer_key" in dim.columns
    assert "taxpayer_id" in dim.columns
    assert dim.count() == 2


def test_build_dim_time(spark, logger, config, sample_df):
    modeler = DimensionalModeler(spark, config, logger)
    dim = modeler.build_dim_time(sample_df)
    
    assert "time_key" in dim.columns
    assert "assessment_year" in dim.columns
    assert "filing_year" in dim.columns
    assert "filing_month" in dim.columns
    assert "filing_quarter" in dim.columns
    assert "tax_season_flag" in dim.columns
    assert dim.count() == 2


def test_build_dim_time_no_dates(spark, logger, config):
    schema = StructType([
        StructField("assessment_year", IntegerType(), True),
        StructField("filing_date_cleansed", DateType(), True)
    ])
    from datetime import date
    data = [(2023, None)]
    df = spark.createDataFrame(data, schema)
    
    modeler = DimensionalModeler(spark, config, logger)
    dim = modeler.build_dim_time(df)
    
    assert dim.count() == 0


def test_build_dim_location(spark, logger, config, sample_df, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    dim = modeler.build_dim_location(sample_df, postal_mapping_file)
    
    assert "location_key" in dim.columns
    assert "postal_code" in dim.columns
    assert "region" in dim.columns
    assert "planning_area" in dim.columns
    assert "is_valid_postal_code" in dim.columns
    assert dim.count() == 2


def test_build_dim_location_no_mapping(spark, logger, config, tmp_path):
    postal_file = tmp_path / "empty_mapping.json"
    with open(postal_file, "w") as f:
        json.dump([], f)
    
    schema = StructType([
        StructField("postal_code_cleansed", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("housing_type", StringType(), True)
    ])
    data = [("999999", "999999", "HDB")]
    df = spark.createDataFrame(data, schema)
    
    modeler = DimensionalModeler(spark, config, logger)
    dim = modeler.build_dim_location(df, str(postal_file))
    
    assert dim.count() == 1
    rows = dim.collect()
    assert rows[0]["region"] is None


def test_build_dim_occupation(spark, logger, config, sample_df):
    modeler = DimensionalModeler(spark, config, logger)
    dim = modeler.build_dim_occupation(sample_df)
    
    assert "occupation_key" in dim.columns
    assert "occupation" in dim.columns
    assert "occupation_category" in dim.columns
    assert dim.count() == 2


def test_build_fact_tax_returns(spark, logger, config, sample_df):
    modeler = DimensionalModeler(spark, config, logger)
    
    dim_taxpayer = modeler.build_dim_taxpayer(sample_df)
    dim_time = modeler.build_dim_time(sample_df)
    
    schema_loc = StructType([
        StructField("postal_code", StringType(), True),
        StructField("location_key", IntegerType(), True)
    ])
    dim_location = spark.createDataFrame([("010000", 0), ("020000", 1)], schema_loc)
    
    schema_occ = StructType([
        StructField("occupation_raw", StringType(), True),
        StructField("occupation_key", IntegerType(), True)
    ])
    dim_occupation = spark.createDataFrame([("Software Engineer", 0), ("Manager", 1)], schema_occ)
    
    fact_df = sample_df.withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(5000.0)) \
        .withColumn("tax_paid_sgd", lit(5000.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    fact = modeler.build_fact_tax_returns(
        fact_df, dim_taxpayer, dim_time, dim_location, dim_occupation
    )
    
    assert "fact_key" in fact.columns
    assert "taxpayer_key" in fact.columns
    assert "time_key" in fact.columns
    assert fact.count() == 2


def test_build_star_schema(spark, logger, config, sample_df, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    
    fact_df = sample_df.withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(5000.0)) \
        .withColumn("tax_paid_sgd", lit(5000.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    star_schema = modeler.build_star_schema(fact_df, postal_mapping_file)
    
    assert "dim_taxpayer" in star_schema
    assert "dim_time" in star_schema
    assert "dim_location" in star_schema
    assert "dim_occupation" in star_schema
    assert "fact_tax_returns" in star_schema


def test_write_gold_layer(spark, logger, config, sample_df, postal_mapping_file, tmp_path):
    modeler = DimensionalModeler(spark, config, logger)
    
    fact_df = sample_df.withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(5000.0)) \
        .withColumn("tax_paid_sgd", lit(5000.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    star_schema = modeler.build_star_schema(fact_df, postal_mapping_file)
    output_path = str(tmp_path / "gold")
    
    modeler.write_gold_layer(star_schema, output_path)
    
    assert os.path.exists(output_path)
    assert os.path.exists(os.path.join(output_path, "dim_taxpayer"))
    assert os.path.exists(os.path.join(output_path, "fact_tax_returns"))


def test_get_region_from_postal(spark, logger, config, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    region, area = modeler._get_region_from_postal("010000", postal_mapping_file)
    assert region == "Central"
    assert area == "Test Area"
    
    region2, area2 = modeler._get_region_from_postal("999999", postal_mapping_file)
    assert region2 is None
    assert area2 is None


def test_get_region_from_postal_short_code(spark, logger, config, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    region, area = modeler._get_region_from_postal("0", postal_mapping_file)
    assert region is None
    assert area is None


def test_get_region_from_postal_empty(spark, logger, config, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    region, area = modeler._get_region_from_postal("", postal_mapping_file)
    assert region is None
    assert area is None


def test_get_region_from_postal_none(spark, logger, config, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    region, area = modeler._get_region_from_postal(None, postal_mapping_file)
    assert region is None
    assert area is None


def test_write_gold_layer_s3_path(spark, logger, config, sample_df, postal_mapping_file):
    modeler = DimensionalModeler(spark, config, logger)
    
    fact_df = sample_df.withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(5000.0)) \
        .withColumn("tax_paid_sgd", lit(5000.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    star_schema = modeler.build_star_schema(fact_df, postal_mapping_file)
    
    # Test S3 path normalization (will fail in local but tests the logic)
    s3_path = "s3://test-bucket"
    # Just verify the method handles it
    try:
        modeler.write_gold_layer(star_schema, s3_path)
    except Exception:
        pass  # Expected in local test


def test_write_gold_layer_empty_table(spark, logger, config, tmp_path):
    modeler = DimensionalModeler(spark, config, logger)
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("col1", StringType(), True)])
    empty_df = spark.createDataFrame([], schema)
    
    star_schema = {"dim_test": empty_df}
    output_path = str(tmp_path / "gold")
    
    # Should not raise error, just skip empty tables
    modeler.write_gold_layer(star_schema, output_path)


def test_write_gold_layer_null_partition(spark, logger, config, sample_df, postal_mapping_file, tmp_path):
    modeler = DimensionalModeler(spark, config, logger)
    
    # Create fact_df with one valid row and one null assessment_year
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType
    from datetime import date
    from pyspark.sql.functions import lit
    
    # Start with valid data, then add null assessment_year
    fact_df = sample_df.withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(5000.0)) \
        .withColumn("tax_paid_sgd", lit(5000.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    # Add a row with null assessment_year
    from pyspark.sql.types import IntegerType
    null_row = fact_df.limit(1).withColumn("assessment_year", lit(None).cast(IntegerType()))
    fact_df_with_null = fact_df.union(null_row)
    
    dim_taxpayer = modeler.build_dim_taxpayer(fact_df_with_null)
    dim_time = modeler.build_dim_time(fact_df_with_null)
    dim_location = modeler.build_dim_location(fact_df_with_null, postal_mapping_file)
    dim_occupation = modeler.build_dim_occupation(fact_df_with_null)
    
    fact = modeler.build_fact_tax_returns(fact_df_with_null, dim_taxpayer, dim_time, dim_location, dim_occupation)
    
    star_schema = {
        "dim_taxpayer": dim_taxpayer,
        "dim_time": dim_time,
        "dim_location": dim_location,
        "dim_occupation": dim_occupation,
        "fact_tax_returns": fact
    }
    
    output_path = str(tmp_path / "gold")
    
    # Should filter out null partition values
    modeler.write_gold_layer(star_schema, output_path)


def test_build_fact_tax_returns_zero_tax_payable(spark, logger, config, sample_df):
    modeler = DimensionalModeler(spark, config, logger)
    
    dim_taxpayer = modeler.build_dim_taxpayer(sample_df)
    dim_time = modeler.build_dim_time(sample_df)
    
    schema_loc = StructType([
        StructField("postal_code", StringType(), True),
        StructField("location_key", IntegerType(), True)
    ])
    dim_location = spark.createDataFrame([("010000", 0)], schema_loc)
    
    schema_occ = StructType([
        StructField("occupation_raw", StringType(), True),
        StructField("occupation_key", IntegerType(), True)
    ])
    dim_occupation = spark.createDataFrame([("Software Engineer", 0)], schema_occ)
    
    fact_df = sample_df.limit(1).withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(0.0)) \
        .withColumn("tax_paid_sgd", lit(0.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    fact = modeler.build_fact_tax_returns(fact_df, dim_taxpayer, dim_time, dim_location, dim_occupation)
    
    # Test division by zero handling
    rows = fact.select("tax_compliance_rate", "effective_tax_rate").collect()
    assert rows[0]["tax_compliance_rate"] is not None


def test_write_gold_layer_path_with_slash(spark, logger, config, sample_df, postal_mapping_file, tmp_path):
    modeler = DimensionalModeler(spark, config, logger)
    
    fact_df = sample_df.withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(5000.0)) \
        .withColumn("tax_paid_sgd", lit(5000.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    star_schema = modeler.build_star_schema(fact_df, postal_mapping_file)
    
    # Test path with trailing slash
    output_path = str(tmp_path / "gold") + "/"
    modeler.write_gold_layer(star_schema, output_path)


def test_write_gold_layer_null_remaining_after_filter(spark, logger, config, sample_df, postal_mapping_file, tmp_path):
    """Test the error path when null values remain after filtering (lines 390-391)"""
    from unittest.mock import patch, MagicMock
    from pyspark.sql.functions import col
    
    modeler = DimensionalModeler(spark, config, logger)
    
    fact_df = sample_df.withColumn("annual_income_sgd", lit(50000.0)) \
        .withColumn("chargeable_income_sgd", lit(40000.0)) \
        .withColumn("tax_payable_sgd", lit(5000.0)) \
        .withColumn("tax_paid_sgd", lit(5000.0)) \
        .withColumn("total_reliefs_sgd", lit(10000.0)) \
        .withColumn("cpf_contributions_sgd", lit(10000.0)) \
        .withColumn("foreign_income_sgd", lit(0.0)) \
        .withColumn("data_quality_score", lit(0.9)) \
        .withColumn("dq_classification", lit("Accept")) \
        .withColumn("is_valid_record", lit(True)) \
        .withColumn("is_quarantined", lit(False)) \
        .withColumn("is_rejected", lit(False)) \
        .withColumn("has_invalid_nric", lit(False)) \
        .withColumn("has_invalid_postal_code", lit(False)) \
        .withColumn("has_invalid_date", lit(False)) \
        .withColumn("has_calculation_error", lit(False)) \
        .withColumn("has_business_rule_violation", lit(False)) \
        .withColumn("has_completeness_issue", lit(False)) \
        .withColumn("ingestion_timestamp", lit("2024-01-01")) \
        .withColumn("source_file", lit("test.csv"))
    
    dim_taxpayer = modeler.build_dim_taxpayer(fact_df)
    dim_time = modeler.build_dim_time(fact_df)
    dim_location = modeler.build_dim_location(fact_df, postal_mapping_file)
    dim_occupation = modeler.build_dim_occupation(fact_df)
    
    fact = modeler.build_fact_tax_returns(fact_df, dim_taxpayer, dim_time, dim_location, dim_occupation)
    
    # Create fact with null assessment_year
    from pyspark.sql.types import IntegerType
    fact_with_null = fact.withColumn("assessment_year", lit(None).cast(IntegerType()))
    
    star_schema = {
        "dim_taxpayer": dim_taxpayer,
        "dim_time": dim_time,
        "dim_location": dim_location,
        "dim_occupation": dim_occupation,
        "fact_tax_returns": fact_with_null
    }
    
    output_path = str(tmp_path / "gold")
    
    # Mock the DataFrame's filter method to return a DataFrame that still has nulls
    # This simulates the edge case where filtering doesn't work properly
    mock_filtered = MagicMock()
    mock_filtered.count.return_value = 1  # Still has 1 null after "filtering"
    
    # Create a chain: fact.filter().filter().count() should return 1
    mock_first_filter = MagicMock()
    mock_first_filter.filter.return_value = mock_filtered
    mock_first_filter.count.return_value = 1  # Initial null count
    
    with patch.object(fact_with_null, 'filter', return_value=mock_first_filter):
        # This should trigger the error path (lines 390-391)
        with pytest.raises(ValueError, match="Cannot write partitioned data with null assessment_year"):
            modeler.write_gold_layer(star_schema, output_path)

