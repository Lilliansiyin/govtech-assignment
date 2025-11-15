from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, udf
from pyspark.sql.types import BooleanType
from src.rules.base_rule import BaseRule
from src.utils.date_parser import compare_dates


class CS001_DecisionDateAfterApplicationDate(BaseRule):
    def __init__(self):
        super().__init__("CS001", "decision_date >= application_date", "Consistency")
        self._udf = udf(compare_dates, BooleanType())
    
    def evaluate(self, df: DataFrame) -> col:
        # Compare dates: decision_date should be >= application_date
        # Returns None if either cannot be parsed, which we treat as fail
        date_comparison = self._udf(col("decision_date"), col("application_date"))
        
        return when(
            col("decision_date").isNotNull() & col("application_date").isNotNull(),
            when(date_comparison.isNull(), lit(False)).otherwise(date_comparison)
        ).otherwise(lit(True))


class CS002_ApprovedAmountLessThanRequested(BaseRule):
    def __init__(self):
        super().__init__("CS002", "approved_amount <= requested_amount when Approved", "Consistency")
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("application_status") == "Approved",
            (col("approved_amount") <= col("requested_amount")) &
            (col("approved_amount") > 0)
        ).otherwise(lit(True))


class CS003_DecisionDateNullWhenPending(BaseRule):
    def __init__(self):
        super().__init__("CS003", "decision_date NULL when Pending", "Consistency")
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("application_status") == "Pending",
            col("decision_date").isNull()
        ).otherwise(lit(True))


class CS004_DecisionDateNotNullWhenDecided(BaseRule):
    def __init__(self):
        super().__init__("CS004", "decision_date NOT NULL when Approved/Rejected", "Consistency")
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("application_status").isin(["Approved", "Rejected"]),
            col("decision_date").isNotNull()
        ).otherwise(lit(True))


def get_consistency_rules() -> list:
    return [
        CS001_DecisionDateAfterApplicationDate(),
        CS002_ApprovedAmountLessThanRequested(),
        CS003_DecisionDateNullWhenPending(),
        CS004_DecisionDateNotNullWhenDecided(),
    ]

