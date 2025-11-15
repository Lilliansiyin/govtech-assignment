from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import BooleanType, DecimalType, StringType

from src.utils.logger import StructuredLogger


class QualityScorer:
    def __init__(self, config: Dict, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.dq_config = config.get("data_quality", {})
        self.weights = self.dq_config.get("weights", {})
        self.thresholds = self.dq_config.get("thresholds", {})

    def calculate_dq_score(self, df: DataFrame) -> DataFrame:
        nric_weight = self.weights.get("nric", 0.25)
        postal_weight = self.weights.get("postal_code", 0.15)
        date_weight = self.weights.get("filing_date", 0.20)
        calc_weight = self.weights.get("tax_calculation", 0.15)
        cpf_weight = self.weights.get("cpf", 0.10)
        completeness_weight = self.weights.get("completeness", 0.15)

        dq_score = (
            col("_nric_score") * nric_weight
            + col("_postal_score") * postal_weight
            + col("_date_score") * date_weight
            + col("_calc_score") * calc_weight
            + col("_cpf_score") * cpf_weight
            + col("_completeness_score") * completeness_weight
        )

        df = df.withColumn("data_quality_score", dq_score.cast(DecimalType(3, 2)))

        return df

    def classify_records(self, df: DataFrame) -> DataFrame:
        accept_threshold = self.thresholds.get("accept", 0.8)
        quarantine_threshold = self.thresholds.get("quarantine", 0.6)

        df = df.withColumn(
            "dq_classification",
            when(col("data_quality_score") >= accept_threshold, "Accept")
            .when(col("data_quality_score") >= quarantine_threshold, "Quarantine")
            .otherwise("Reject"),
        )

        df = df.withColumn(
            "is_valid_record",
            (col("data_quality_score") >= accept_threshold).cast(BooleanType()),
        )

        df = df.withColumn(
            "is_quarantined",
            (
                (col("data_quality_score") >= quarantine_threshold)
                & (col("data_quality_score") < accept_threshold)
            ).cast(BooleanType()),
        )

        df = df.withColumn(
            "is_rejected",
            (col("data_quality_score") < quarantine_threshold).cast(BooleanType()),
        )

        return df

    def score_and_classify(self, df: DataFrame) -> DataFrame:
        df = self.calculate_dq_score(df)
        df = self.classify_records(df)

        accept_count = df.filter(col("dq_classification") == "Accept").count()
        quarantine_count = df.filter(col("dq_classification") == "Quarantine").count()
        reject_count = df.filter(col("dq_classification") == "Reject").count()

        self.logger.info(
            "Data quality classification",
            accept=accept_count,
            quarantine=quarantine_count,
            reject=reject_count,
        )

        return df
