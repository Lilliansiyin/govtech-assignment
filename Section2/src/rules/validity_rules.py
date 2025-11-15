from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, abs as spark_abs, upper, rlike
from src.rules.base_rule import BaseRule


class V001_CitizenNricFormat(BaseRule):
    def __init__(self):
        super().__init__("V001", "citizen_nric matches NRIC format", "Validity")
    
    def evaluate(self, df: DataFrame) -> col:
        # Check not null and matches pattern
        return (
            col("citizen_nric").isNotNull() &
            col("citizen_nric").rlike("^[STFG]\\d{7}[A-Z]$")
        )


class V002_HouseholdIncomeRange(BaseRule):
    def __init__(self):
        super().__init__("V002", "household_income in valid range", "Validity")
    
    def evaluate(self, df: DataFrame) -> col:
        return (
            (col("household_income") >= 0) &
            (spark_abs(col("household_income")) < 10000000)
        )


class V003_RequestedAmountRange(BaseRule):
    def __init__(self):
        super().__init__("V003", "requested_amount >= 0", "Validity")
    
    def evaluate(self, df: DataFrame) -> col:
        return col("requested_amount") >= 0


class V004_ApprovedAmountRange(BaseRule):
    def __init__(self):
        super().__init__("V004", "approved_amount >= 0 when not null", "Validity")
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("approved_amount").isNotNull(),
            col("approved_amount") >= 0
        ).otherwise(lit(True))


class V005_HouseholdSizeRange(BaseRule):
    def __init__(self):
        super().__init__("V005", "household_size in valid range (1-20)", "Validity")
    
    def evaluate(self, df: DataFrame) -> col:
        return (
            (col("household_size") >= 1) &
            (col("household_size") <= 20) &
            (col("household_size") == col("household_size").cast("int"))
        )


class V006_ApprovedAmountOnlyWhenApproved(BaseRule):
    def __init__(self):
        super().__init__("V006", "approved_amount only when Approved", "Validity")
    
    def evaluate(self, df: DataFrame) -> col:
        return when(
            col("application_status") != "Approved",
            col("approved_amount").isNull()
        ).otherwise(lit(True))


class V007_NoPlaceholderValues(BaseRule):
    def __init__(self):
        super().__init__("V007", "No placeholder values", "Validity")
    
    def evaluate(self, df: DataFrame) -> col:
        # Check for placeholder values in critical fields
        placeholder_patterns = [
            "NOT_A_DATE",
            "INVALID_DATE",
            "INVALID_NRIC",
            "NULL",
            "N/A",
            "NA"
        ]
        
        # Check application_date
        date_check = lit(True)
        for pattern in placeholder_patterns:
            date_check = date_check & (
                ~upper(col("application_date")).contains(pattern)
            )
        
        # Check decision_date
        decision_date_check = when(
            col("decision_date").isNotNull(),
            lit(True)
        ).otherwise(lit(True))
        for pattern in placeholder_patterns:
            decision_date_check = decision_date_check & (
                when(
                    col("decision_date").isNotNull(),
                    ~upper(col("decision_date")).contains(pattern)
                ).otherwise(lit(True))
            )
        
        # Check citizen_nric
        nric_check = when(
            col("citizen_nric").isNotNull(),
            lit(True)
        ).otherwise(lit(True))
        for pattern in placeholder_patterns:
            nric_check = nric_check & (
                when(
                    col("citizen_nric").isNotNull(),
                    ~upper(col("citizen_nric")).contains(pattern)
                ).otherwise(lit(True))
            )
        
        return date_check & decision_date_check & nric_check


def get_validity_rules() -> list:
    return [
        V001_CitizenNricFormat(),
        V002_HouseholdIncomeRange(),
        V003_RequestedAmountRange(),
        V004_ApprovedAmountRange(),
        V005_HouseholdSizeRange(),
        V006_ApprovedAmountOnlyWhenApproved(),
        V007_NoPlaceholderValues(),
    ]

