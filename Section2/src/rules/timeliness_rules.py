from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import IntegerType
from src.rules.base_rule import BaseRule
from src.utils.date_parser import parse_date_flexible


def _days_between(date1_str, date2_str):
    """Calculate days between two dates."""
    date1 = parse_date_flexible(date1_str)
    date2 = parse_date_flexible(date2_str)
    if date1 is None or date2 is None:
        return None
    return (date1 - date2).days


def _days_old(date_str):
    """Calculate days since a given date."""
    parsed_date = parse_date_flexible(date_str)
    if parsed_date is None:
        return None
    return (datetime.now() - parsed_date).days


class T001_DecisionLagWithinSLA(BaseRule):
    def __init__(self, sla_days: int = 30):
        super().__init__("T001", f"decision_lag <= {sla_days} days", "Timeliness")
        self.sla_days = sla_days
        self._days_diff_udf = None
    
    def evaluate(self, df: DataFrame) -> col:
        # Create UDF lazily using the DataFrame's SparkSession to ensure SparkContext is available
        if self._days_diff_udf is None:
            self._days_diff_udf = udf(_days_between, IntegerType())
        
        days_diff = self._days_diff_udf(col("decision_date"), col("application_date"))
        
        return when(
            col("application_status").isin(["Approved", "Rejected"]) &
            col("decision_date").isNotNull() &
            col("application_date").isNotNull(),
            when(
                days_diff.isNull(),
                lit(False)  # Fail if dates can't be parsed
            ).otherwise(
                (days_diff <= self.sla_days) & (days_diff >= 0)
            )
        ).otherwise(lit(True))


class T002_DataFreshness(BaseRule):
    def __init__(self, max_age_years: int = 5):
        super().__init__("T002", f"application_date not > {max_age_years} years old", "Timeliness")
        self.max_age_days = max_age_years * 365
        self._days_old_udf = None
    
    def evaluate(self, df: DataFrame) -> col:
        # Create UDF lazily using the DataFrame's SparkSession to ensure SparkContext is available
        if self._days_old_udf is None:
            self._days_old_udf = udf(_days_old, IntegerType())
        
        days_old_col = self._days_old_udf(col("application_date"))
        
        return when(
            col("application_date").isNotNull(),
            when(
                days_old_col.isNull(),
                lit(False)  # Fail if date can't be parsed
            ).otherwise(
                days_old_col <= self.max_age_days
            )
        ).otherwise(lit(True))


def get_timeliness_rules(sla_days: int = 30, max_age_years: int = 5) -> list:
    return [
        T001_DecisionLagWithinSLA(sla_days=sla_days),
        T002_DataFreshness(max_age_years=max_age_years),
    ]

