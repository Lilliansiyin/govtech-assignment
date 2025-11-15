from typing import Dict, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs, col, isnan, lit, regexp_extract, when
from pyspark.sql.types import BooleanType

from src.utils.logger import StructuredLogger


class Validator:
    def __init__(self, config: Dict, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.validation_rules = config.get("validation", {})

    def validate_nric(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        pattern = self.validation_rules.get("nric", {}).get(
            "pattern", "^[STFG]\\d{7}[A-Z]$"
        )
        allow_empty = self.validation_rules.get("nric", {}).get("allow_empty", False)

        if allow_empty:
            is_valid = (col("nric").isNull() | (col("nric") == "")) | col("nric").rlike(
                pattern
            )
        else:
            is_valid = (
                col("nric").isNotNull()
                & (col("nric") != "")
                & col("nric").rlike(pattern)
            )

        df = df.withColumn("has_invalid_nric", ~is_valid.cast(BooleanType()))
        return df, is_valid

    def validate_postal_code(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        pattern = self.validation_rules.get("postal_code", {}).get(
            "pattern", "^\\d{6}$"
        )
        min_value = self.validation_rules.get("postal_code", {}).get("min_value", 10000)
        max_value = self.validation_rules.get("postal_code", {}).get(
            "max_value", 829999
        )

        is_numeric = col("postal_code").rlike("^\\d+$")
        is_valid_format = col("postal_code").rlike(pattern)

        postal_code_int = when(is_numeric, col("postal_code").cast("int")).otherwise(
            lit(-1)
        )
        is_in_range = (postal_code_int >= min_value) & (postal_code_int <= max_value)

        is_valid = (
            col("postal_code").isNotNull()
            & (col("postal_code") != "")
            & is_valid_format
            & is_in_range
        )

        df = df.withColumn("has_invalid_postal_code", ~is_valid.cast(BooleanType()))
        return df, is_valid

    def validate_filing_date(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        iso_pattern = self.validation_rules.get("date", {}).get(
            "iso_pattern", "^\\d{4}-\\d{2}-\\d{2}$"
        )
        ddmmyyyy_pattern = self.validation_rules.get("date", {}).get(
            "ddmmyyyy_pattern", "^\\d{1,2}/\\d{1,2}/\\d{4}$"
        )

        is_iso = col("filing_date").rlike(iso_pattern)
        is_ddmmyyyy = col("filing_date").rlike(ddmmyyyy_pattern)
        has_valid_format = is_iso | is_ddmmyyyy

        assessment_year = col("assessment_year").cast("int")
        filing_year_extracted = (
            when(is_iso, regexp_extract(col("filing_date"), r"^(\d{4})", 1).cast("int"))
            .when(
                is_ddmmyyyy,
                regexp_extract(col("filing_date"), r"(\d{4})$", 1).cast("int"),
            )
            .otherwise(lit(-1))
        )

        is_after_assessment = filing_year_extracted >= assessment_year

        is_valid = (
            col("filing_date").isNotNull()
            & (col("filing_date") != "")
            & has_valid_format
            & is_after_assessment
        )

        df = df.withColumn("has_invalid_date", ~is_valid.cast(BooleanType()))
        return df, is_valid

    def validate_tax_calculation(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        tolerance = self.validation_rules.get("tax_calculation", {}).get(
            "tolerance", 0.1
        )

        expected_chargeable = col("annual_income_sgd") - col("total_reliefs_sgd")
        difference = abs(col("chargeable_income_sgd") - expected_chargeable)

        is_valid = difference <= tolerance

        df = df.withColumn("has_calculation_error", ~is_valid.cast(BooleanType()))
        return df, is_valid

    def validate_cpf_rule(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        non_resident_must_be_zero = self.validation_rules.get("cpf", {}).get(
            "non_resident_must_be_zero", True
        )

        if non_resident_must_be_zero:
            is_valid = when(
                col("residential_status") == "Non-Resident",
                col("cpf_contributions_sgd") == 0,
            ).otherwise(lit(True))
        else:
            is_valid = lit(True)

        df = df.withColumn("has_business_rule_violation", ~is_valid.cast(BooleanType()))
        return df, is_valid

    def validate_completeness(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        excluded_columns = {
            "ingestion_timestamp",
            "source_file",
            "raw_record_hash",
            "has_invalid_nric",
            "has_invalid_postal_code",
            "has_invalid_date",
            "has_calculation_error",
            "has_business_rule_violation",
            "has_completeness_issue",
        }

        numeric_columns = {
            "annual_income_sgd",
            "chargeable_income_sgd",
            "tax_payable_sgd",
            "tax_paid_sgd",
            "total_reliefs_sgd",
            "cpf_contributions_sgd",
            "foreign_income_sgd",
            "number_of_dependents",
            "assessment_year",
        }

        all_columns = df.columns
        completeness_checks = []

        for col_name in all_columns:
            if col_name not in excluded_columns:
                check = col(col_name).isNull() | (col(col_name) == "")

                if col_name in numeric_columns:
                    check = check | isnan(col(col_name))

                completeness_checks.append(check)

        if not completeness_checks:
            is_valid = lit(True)
            has_missing = lit(False)
        else:
            has_missing = completeness_checks[0]
            for check in completeness_checks[1:]:
                has_missing = has_missing | check
            is_valid = ~has_missing

        df = df.withColumn("has_completeness_issue", has_missing.cast(BooleanType()))
        return df, is_valid

    def apply_all_validations(self, df: DataFrame) -> DataFrame:
        df, nric_valid = self.validate_nric(df)
        df, postal_valid = self.validate_postal_code(df)
        df, date_valid = self.validate_filing_date(df)
        df, calc_valid = self.validate_tax_calculation(df)
        df, cpf_valid = self.validate_cpf_rule(df)
        df, completeness_valid = self.validate_completeness(df)

        df = (
            df.withColumn("_nric_score", when(nric_valid, 1.0).otherwise(0.0))
            .withColumn("_postal_score", when(postal_valid, 1.0).otherwise(0.0))
            .withColumn("_date_score", when(date_valid, 1.0).otherwise(0.0))
            .withColumn("_calc_score", when(calc_valid, 1.0).otherwise(0.0))
            .withColumn("_cpf_score", when(cpf_valid, 1.0).otherwise(0.0))
            .withColumn(
                "_completeness_score", when(completeness_valid, 1.0).otherwise(0.0)
            )
        )

        return df
