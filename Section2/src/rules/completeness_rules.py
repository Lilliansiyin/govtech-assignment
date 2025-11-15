from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, trim
from src.rules.base_rule import BaseRule


class C001_ApplicationIdNotNull(BaseRule):
    def __init__(self):
        super().__init__("C001", "application_id is not null/empty", "Completeness")
    
    def evaluate(self, df: DataFrame) -> col:
        # Check not null and not empty string
        # Use cast to string to avoid type mismatch issues
        return (col("application_id").isNotNull()) & (trim(col("application_id").cast("string")) != "")


class C002_ApplicationDateNotNull(BaseRule):
    def __init__(self):
        super().__init__("C002", "application_date is not null/empty", "Completeness")
    
    def evaluate(self, df: DataFrame) -> col:
        # Check not null and not empty string
        # Use cast to string to avoid type mismatch issues
        return (col("application_date").isNotNull()) & (trim(col("application_date").cast("string")) != "")


class C003_ApplicationStatusNotNull(BaseRule):
    def __init__(self):
        super().__init__("C003", "application_status is not null/empty", "Completeness")
    
    def evaluate(self, df: DataFrame) -> col:
        # Check not null and not empty string
        # Use cast to string to avoid type mismatch issues
        return (col("application_status").isNotNull()) & (trim(col("application_status").cast("string")) != "")


class C004_RequestedAmountNotNull(BaseRule):
    def __init__(self):
        super().__init__("C004", "requested_amount is not null", "Completeness")
    
    def evaluate(self, df: DataFrame) -> col:
        return col("requested_amount").isNotNull()


class C005_ApprovedAmountNotNullWhenApproved(BaseRule):
    def __init__(self):
        super().__init__("C005", "approved_amount is not null when Approved", "Completeness")
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("application_status") == "Approved",
            col("approved_amount").isNotNull()
        ).otherwise(lit(True))


class C006_DecisionDateNotNullWhenDecided(BaseRule):
    def __init__(self):
        super().__init__("C006", "decision_date is not null when Approved/Rejected", "Completeness")
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("application_status").isin(["Approved", "Rejected"]),
            col("decision_date").isNotNull()
        ).otherwise(lit(True))


def get_completeness_rules() -> list:
    return [
        C001_ApplicationIdNotNull(),
        C002_ApplicationDateNotNull(),
        C003_ApplicationStatusNotNull(),
        C004_RequestedAmountNotNull(),
        C005_ApprovedAmountNotNullWhenApproved(),
        C006_DecisionDateNotNullWhenDecided(),
    ]

