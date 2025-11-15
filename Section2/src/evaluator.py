from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from src.rules.base_rule import BaseRule
import logging

logger = logging.getLogger(__name__)


class RuleEvaluator:
    def __init__(self, rules: List[BaseRule]):
        self.rules = rules
        logger.info(f"Initialized RuleEvaluator with {len(rules)} rules")
    
    def evaluate_all(self, df: DataFrame) -> DataFrame:
        result_df = df
        
        for rule in self.rules:
            try:
                rule_column = rule.evaluate(result_df)
                result_df = result_df.withColumn(
                    f"rule_{rule.rule_id}_pass",
                    rule_column
                )
                logger.debug(f"Evaluated rule {rule.rule_id}: {rule.rule_name}")
            except Exception as e:
                logger.error(f"Error evaluating rule {rule.rule_id}: {str(e)}")
                # Mark all rows as failing for this rule if evaluation fails
                from pyspark.sql.functions import lit
                result_df = result_df.withColumn(
                    f"rule_{rule.rule_id}_pass",
                    lit(False)
                )
        
        return result_df
    
    def get_rule_results(self, df: DataFrame) -> Dict[str, Dict]:
        results = {}
        total_rows = df.count()
        
        for rule in self.rules:
            rule_col = f"rule_{rule.rule_id}_pass"
            if rule_col in df.columns:
                passing_rows = df.filter(col(rule_col) == True).count()
                failing_rows = total_rows - passing_rows
                
                results[rule.rule_id] = {
                    "rule_id": rule.rule_id,
                    "rule_name": rule.rule_name,
                    "dimension": rule.dimension,
                    "total_rows": total_rows,
                    "passing_rows": passing_rows,
                    "failing_rows": failing_rows,
                    "pass_rate": (passing_rows / total_rows * 100) if total_rows > 0 else 0.0
                }
        
        return results
    
    def get_rule_results_df(self, df: DataFrame, execution_id: str) -> DataFrame:
        """
        Generate DataFrame matching dq_rule_results table structure.
        
        Args:
            df: DataFrame with rule evaluation columns
            execution_id: Unique identifier for this execution
            
        Returns:
            DataFrame with columns: execution_id, rule_id, dimension_name, rule_name,
            total_rows, passing_rows, failing_rows, failure_rate
        """
        total_rows = df.count()
        rule_results = []
        
        for rule in self.rules:
            rule_col = f"rule_{rule.rule_id}_pass"
            if rule_col in df.columns:
                passing_rows = df.filter(col(rule_col) == True).count()
                failing_rows = total_rows - passing_rows
                failure_rate = (failing_rows / total_rows * 100) if total_rows > 0 else 0.0
                
                rule_results.append({
                    "execution_id": execution_id,
                    "rule_id": rule.rule_id,
                    "dimension_name": rule.dimension,
                    "rule_name": rule.rule_name,
                    "total_rows": total_rows,
                    "passing_rows": passing_rows,
                    "failing_rows": failing_rows,
                    "failure_rate": round(failure_rate, 2)
                })
            else:
                logger.warning(f"Rule column {rule_col} not found, marking as failed")
                rule_results.append({
                    "execution_id": execution_id,
                    "rule_id": rule.rule_id,
                    "dimension_name": rule.dimension,
                    "rule_name": rule.rule_name,
                    "total_rows": total_rows,
                    "passing_rows": 0,
                    "failing_rows": total_rows,
                    "failure_rate": 100.0
                })
        
        # Create DataFrame matching dq_rule_results table structure
        schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("rule_id", StringType(), False),
            StructField("dimension_name", StringType(), False),
            StructField("rule_name", StringType(), False),
            StructField("total_rows", IntegerType(), False),
            StructField("passing_rows", IntegerType(), False),
            StructField("failing_rows", IntegerType(), False),
            StructField("failure_rate", DoubleType(), False)
        ])
        
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active Spark session found")
        
        result_df = spark.createDataFrame(rule_results, schema=schema)
        
        return result_df

