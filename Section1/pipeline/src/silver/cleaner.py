from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, regexp_replace, to_date, when
from pyspark.sql.types import StringType

from src.utils.logger import StructuredLogger


class DataCleaner:
    def __init__(self, config: Dict, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.validation_rules = config.get("validation", {})

    def clean_postal_code(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "postal_code_cleansed", regexp_replace(col("postal_code"), "[^0-9]", "")
        )

        df = df.withColumn(
            "postal_code_cleansed",
            when(
                (col("postal_code_cleansed").rlike("^\\d{6}$"))
                & (col("postal_code_cleansed").cast("int").between(10000, 829999)),
                col("postal_code_cleansed"),
            ).otherwise(col("postal_code")),
        )

        return df

    def clean_filing_date(self, df: DataFrame) -> DataFrame:
        iso_format = self.validation_rules.get("date", {}).get(
            "iso_format", "yyyy-MM-dd"
        )
        ddmmyyyy_format = self.validation_rules.get("date", {}).get(
            "ddmmyyyy_format", "dd/MM/yyyy"
        )

        df = df.withColumn(
            "filing_date_cleansed",
            when(
                col("filing_date").rlike("^\\d{4}-\\d{2}-\\d{2}$"),
                to_date(col("filing_date"), iso_format),
            )
            .when(
                col("filing_date").rlike("^\\d{1,2}/\\d{1,2}/\\d{4}$"),
                to_date(col("filing_date"), ddmmyyyy_format),
            )
            .otherwise(lit(None)),
        )

        return df

    def clean_nric(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "nric_cleansed",
            when(col("nric").isNotNull() & (col("nric") != ""), col("nric")).otherwise(
                lit(None)
            ),
        )

        return df

    def clean_all(self, df: DataFrame) -> DataFrame:
        df = self.clean_postal_code(df)
        df = self.clean_filing_date(df)
        df = self.clean_nric(df)

        return df
