import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import lit

from src.silver.quality_scorer import QualityScorer
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
        "data_quality": {
            "weights": {
                "nric": 0.25,
                "postal_code": 0.15,
                "filing_date": 0.20,
                "tax_calculation": 0.15,
                "cpf": 0.10,
                "completeness": 0.15
            },
            "thresholds": {
                "accept": 0.8,
                "quarantine": 0.6
            }
        }
    }


@pytest.fixture
def sample_df_with_scores(spark):
    schema = StructType([
        StructField("_nric_score", DoubleType(), True),
        StructField("_postal_score", DoubleType(), True),
        StructField("_date_score", DoubleType(), True),
        StructField("_calc_score", DoubleType(), True),
        StructField("_cpf_score", DoubleType(), True),
        StructField("_completeness_score", DoubleType(), True)
    ])
    data = [
        (1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
        (0.5, 0.5, 0.5, 0.5, 0.5, 0.5),
        (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    ]
    return spark.createDataFrame(data, schema)


def test_quality_scorer_init(logger, config):
    scorer = QualityScorer(config, logger)
    assert scorer.config == config
    assert len(scorer.weights) > 0
    assert len(scorer.thresholds) > 0


def test_calculate_dq_score(spark, logger, config, sample_df_with_scores):
    scorer = QualityScorer(config, logger)
    result_df = scorer.calculate_dq_score(sample_df_with_scores)
    
    assert "data_quality_score" in result_df.columns
    rows = result_df.collect()
    assert rows[0]["data_quality_score"] == 1.0
    assert rows[1]["data_quality_score"] == 0.5


def test_classify_records(spark, logger, config):
    schema = StructType([StructField("data_quality_score", DoubleType(), True)])
    data = [(0.9,), (0.7,), (0.5,)]
    df = spark.createDataFrame(data, schema)
    
    scorer = QualityScorer(config, logger)
    result_df = scorer.classify_records(df)
    
    assert "dq_classification" in result_df.columns
    assert "is_valid_record" in result_df.columns
    assert "is_quarantined" in result_df.columns
    assert "is_rejected" in result_df.columns
    
    rows = result_df.collect()
    assert rows[0]["dq_classification"] == "Accept"
    assert rows[1]["dq_classification"] == "Quarantine"
    assert rows[2]["dq_classification"] == "Reject"


def test_score_and_classify(spark, logger, config, sample_df_with_scores):
    scorer = QualityScorer(config, logger)
    result_df = scorer.score_and_classify(sample_df_with_scores)
    
    assert "data_quality_score" in result_df.columns
    assert "dq_classification" in result_df.columns
    assert result_df.count() == 3


def test_classify_edge_cases(spark, logger, config):
    schema = StructType([StructField("data_quality_score", DoubleType(), True)])
    data = [(0.8,), (0.6,)]
    df = spark.createDataFrame(data, schema)
    
    scorer = QualityScorer(config, logger)
    result_df = scorer.classify_records(df)
    
    rows = result_df.collect()
    assert rows[0]["dq_classification"] == "Accept"
    assert rows[1]["dq_classification"] == "Quarantine"

