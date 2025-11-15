from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, upper, udf
from pyspark.sql.types import BooleanType
from src.rules.base_rule import BaseRule
from src.utils.date_parser import is_valid_iso_date, can_canonicalize_to_iso


class CF001_DecisionDateIsoFormat(BaseRule):
    def __init__(self):
        super().__init__("CF001", "decision_date follows YYYY-MM-DD format", "Conformity")
        self._udf = udf(is_valid_iso_date, BooleanType())
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("decision_date").isNotNull(),
            self._udf(col("decision_date"))
        ).otherwise(lit(True))


class CF002_ApplicationDateIsoOrCanonicalizable(BaseRule):
    def __init__(self):
        super().__init__("CF002", "application_date is ISO or canonicalizable", "Conformity")
        self._is_iso_udf = udf(is_valid_iso_date, BooleanType())
        self._can_canonicalize_udf = udf(can_canonicalize_to_iso, BooleanType())
    
    def evaluate(self, df: DataFrame) -> col:
        # Check if it's ISO format OR can be canonicalized
        is_iso = self._is_iso_udf(col("application_date"))
        can_canonicalize = self._can_canonicalize_udf(col("application_date"))
        
        return is_iso | can_canonicalize


class CF003_ApplicationStatusCanonical(BaseRule):
    def __init__(self):
        super().__init__("CF003", "application_status matches canonical values", "Conformity")
    
    def evaluate(self, df: DataFrame) -> col:
        return col("application_status").isin(["Pending", "Approved", "Rejected"])


class CF004_GrantSchemeNameCatalog(BaseRule):
    def __init__(self):
        super().__init__("CF004", "grant_scheme_name matches catalog", "Conformity")
    
    def evaluate(self, df: DataFrame) -> col:
        allowed_schemes = [
            "EDUCATION BURSARY",
            "HEALTHCARE SUBSIDY",
            "SKILLS UPGRADING GRANT"
        ]
        return upper(col("grant_scheme_name")).isin(allowed_schemes)


def get_conformity_rules() -> list:
    return [
        CF001_DecisionDateIsoFormat(),
        CF002_ApplicationDateIsoOrCanonicalizable(),
        CF003_ApplicationStatusCanonical(),
        CF004_GrantSchemeNameCatalog(),
    ]

