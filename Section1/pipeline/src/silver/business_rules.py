from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

from src.utils.logger import StructuredLogger


class BusinessRules:
    def __init__(self, config: Dict, logger: StructuredLogger):
        self.config = config
        self.logger = logger

    def apply_cpf_correction(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "cpf_contributions_sgd",
            when(
                (col("residential_status") == "Non-Resident")
                & (col("cpf_contributions_sgd") > 0),
                lit(0.0),
            ).otherwise(col("cpf_contributions_sgd")),
        )

        corrected_count = df.filter(
            (col("residential_status") == "Non-Resident")
            & (col("cpf_contributions_sgd") == 0)
        ).count()

        if corrected_count > 0:
            self.logger.info(
                f"Auto-corrected CPF for {corrected_count} non-resident records"
            )

        return df
