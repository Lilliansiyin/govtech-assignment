import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.silver.cleaner import DataCleaner
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
            "date": {
                "iso_format": "yyyy-MM-dd",
                "ddmmyyyy_format": "dd/MM/yyyy"
            }
        }
    }


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("postal_code", StringType(), True),
        StructField("filing_date", StringType(), True),
        StructField("nric", StringType(), True)
    ])
    data = [
        ("119077", "2024-03-15", "S1234567A"),
        ("ABC123", "15/03/2024", ""),
        ("238882", "invalid-date", None)
    ]
    return spark.createDataFrame(data, schema)


def test_cleaner_init(logger, config):
    cleaner = DataCleaner(config, logger)
    assert cleaner.config == config
    assert cleaner.logger == logger


def test_clean_postal_code(spark, logger, config, sample_df):
    cleaner = DataCleaner(config, logger)
    result_df = cleaner.clean_postal_code(sample_df)
    
    assert "postal_code_cleansed" in result_df.columns
    rows = result_df.select("postal_code", "postal_code_cleansed").collect()
    assert rows[0]["postal_code_cleansed"] == "119077"
    assert rows[1]["postal_code_cleansed"] == "ABC123"


def test_clean_filing_date(spark, logger, config, sample_df):
    cleaner = DataCleaner(config, logger)
    result_df = cleaner.clean_filing_date(sample_df)
    
    assert "filing_date_cleansed" in result_df.columns
    rows = result_df.select("filing_date", "filing_date_cleansed").collect()
    assert rows[0]["filing_date_cleansed"] is not None
    assert rows[1]["filing_date_cleansed"] is not None


def test_clean_filing_date_ddmmyyyy(spark, logger, config):
    schema = StructType([StructField("filing_date", StringType(), True)])
    data = [("15/03/2024",)]
    df = spark.createDataFrame(data, schema)
    
    cleaner = DataCleaner(config, logger)
    result_df = cleaner.clean_filing_date(df)
    rows = result_df.collect()
    assert rows[0]["filing_date_cleansed"] is not None


def test_clean_nric(spark, logger, config, sample_df):
    cleaner = DataCleaner(config, logger)
    result_df = cleaner.clean_nric(sample_df)
    
    assert "nric_cleansed" in result_df.columns
    rows = result_df.select("nric", "nric_cleansed").collect()
    assert rows[0]["nric_cleansed"] == "S1234567A"
    assert rows[1]["nric_cleansed"] is None


def test_clean_all(spark, logger, config, sample_df):
    cleaner = DataCleaner(config, logger)
    result_df = cleaner.clean_all(sample_df)
    
    assert "postal_code_cleansed" in result_df.columns
    assert "filing_date_cleansed" in result_df.columns
    assert "nric_cleansed" in result_df.columns

