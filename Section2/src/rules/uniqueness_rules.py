from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, row_number
from pyspark.sql.window import Window
from src.rules.base_rule import BaseRule


class U001_ApplicationIdUnique(BaseRule):
    def __init__(self):
        super().__init__("U001", "application_id is unique", "Uniqueness")
    
    def evaluate(self, df: DataFrame) -> col:
        # For uniqueness, check if each application_id appears only once
        # Use window function to count occurrences per application_id
        window_spec = Window.partitionBy("application_id")
        count_per_id = count("*").over(window_spec)
        
        # Row passes if count is 1 (unique)
        return count_per_id == 1


class U002_NoExactDuplicateRows(BaseRule):
    def __init__(self):
        super().__init__("U002", "No exact duplicate rows", "Uniqueness")
    
    def evaluate(self, df: DataFrame) -> col:
        # For exact duplicates, we need to check if a row is unique across all columns
        # Use window function partitioned by all columns, ordered by application_id for determinism
        all_cols = [col(c) for c in df.columns]
        window_spec = Window.partitionBy(*all_cols).orderBy(col("application_id"))
        row_num = row_number().over(window_spec)
        
        # Row passes if it's the first occurrence (row_num == 1)
        return row_num == 1


def get_uniqueness_rules() -> list:
    return [
        U001_ApplicationIdUnique(),
        U002_NoExactDuplicateRows(),
    ]

