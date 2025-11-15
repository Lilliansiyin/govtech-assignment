from typing import List, Dict
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from src.rules.base_rule import BaseRule
import logging

logger = logging.getLogger(__name__)


class DimensionAggregator:
    DIMENSIONS = [
        "Completeness",
        "Uniqueness",
        "Validity",
        "Conformity",
        "Consistency",
        "Timeliness"
    ]
    
    def __init__(self, rules: List[BaseRule]):
        self.rules = rules
        self.rules_by_dimension = self._group_rules_by_dimension()
        logger.info(f"Initialized DimensionAggregator for {len(self.DIMENSIONS)} dimensions")
    
    def _group_rules_by_dimension(self) -> Dict[str, List[BaseRule]]:
        grouped = {}
        for rule in self.rules:
            dimension = rule.dimension
            if dimension not in grouped:
                grouped[dimension] = []
            grouped[dimension].append(rule)
        return grouped
    
    def calculate_dimension_scores(self, df: DataFrame, execution_id: str) -> DataFrame:
        """
        Calculate dimension scores and return DataFrame matching dq_dimension_scores table structure.
        
        Args:
            df: DataFrame with rule evaluation columns
            execution_id: Unique identifier for this execution
            
        Returns:
            DataFrame with columns: execution_id, dimension_name, score, total_rows, 
            passing_rows, failing_rows, rule_count
        """
        total_rows = df.count()
        
        dimension_results = []
        
        for dimension in self.DIMENSIONS:
            if dimension not in self.rules_by_dimension:
                logger.warning(f"No rules found for dimension: {dimension}")
                continue
            
            dimension_rules = self.rules_by_dimension[dimension]
            
            # Create a column that is True only if ALL rules in this dimension pass
            dimension_pass_col = lit(True)
            for rule in dimension_rules:
                rule_col = f"rule_{rule.rule_id}_pass"
                if rule_col in df.columns:
                    dimension_pass_col = dimension_pass_col & col(rule_col)
                else:
                    logger.warning(f"Rule column {rule_col} not found in DataFrame")
                    # If rule column missing, treat as fail
                    dimension_pass_col = dimension_pass_col & lit(False)
            
            # Count passing rows
            passing_rows = df.filter(dimension_pass_col).count()
            failing_rows = total_rows - passing_rows
            score = (passing_rows / total_rows * 100) if total_rows > 0 else 0.0
            
            dimension_results.append({
                "execution_id": execution_id,
                "dimension_name": dimension,
                "score": round(score, 2),
                "total_rows": total_rows,
                "passing_rows": passing_rows,
                "failing_rows": failing_rows,
                "rule_count": len(dimension_rules)
            })
            
            logger.info(
                f"Dimension {dimension}: Score = {score:.2f}% "
                f"({passing_rows}/{total_rows} rows passing all {len(dimension_rules)} rules)"
            )
        
        # Create DataFrame from results matching dq_dimension_scores table structure
        schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("dimension_name", StringType(), False),
            StructField("score", DoubleType(), False),
            StructField("total_rows", IntegerType(), False),
            StructField("passing_rows", IntegerType(), False),
            StructField("failing_rows", IntegerType(), False),
            StructField("rule_count", IntegerType(), False)
        ])
        
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active Spark session found")
        
        result_df = spark.createDataFrame(dimension_results, schema=schema)
        
        return result_df

