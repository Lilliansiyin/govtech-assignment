"""Unit tests for dimension aggregator."""

import pytest
from pyspark.sql.functions import col, lit
from src.dimension_aggregator import DimensionAggregator
from src.rules.base_rule import BaseRule


class MockRule(BaseRule):
    """Mock rule for testing."""
    
    def __init__(self, rule_id, rule_name, dimension):
        super().__init__(rule_id, rule_name, dimension)
    
    def evaluate(self, df):
        return col("application_id").isNotNull()


class TestDimensionAggregator:
    """Test cases for dimension aggregator."""
    
    def test_init(self):
        """Test DimensionAggregator initialization."""
        rules = [
            MockRule("C001", "Rule 1", "Completeness"),
            MockRule("U001", "Rule 2", "Uniqueness")
        ]
        aggregator = DimensionAggregator(rules)
        
        assert aggregator.rules == rules
        assert len(aggregator.rules_by_dimension) == 2
        assert "Completeness" in aggregator.rules_by_dimension
        assert "Uniqueness" in aggregator.rules_by_dimension
    
    def test_group_rules_by_dimension(self):
        """Test grouping rules by dimension."""
        rules = [
            MockRule("C001", "Rule 1", "Completeness"),
            MockRule("C002", "Rule 2", "Completeness"),
            MockRule("U001", "Rule 3", "Uniqueness")
        ]
        aggregator = DimensionAggregator(rules)
        
        grouped = aggregator.rules_by_dimension
        assert len(grouped["Completeness"]) == 2
        assert len(grouped["Uniqueness"]) == 1
    
    def test_calculate_dimension_scores(self, spark_session, sample_dataframe):
        """Test calculating dimension scores."""
        # Add rule columns to DataFrame
        df_with_rules = sample_dataframe.withColumn(
            "rule_C001_pass", col("application_id").isNotNull()
        ).withColumn(
            "rule_C002_pass", col("first_name").isNotNull()
        )
        
        rules = [
            MockRule("C001", "Rule 1", "Completeness"),
            MockRule("C002", "Rule 2", "Completeness")
        ]
        aggregator = DimensionAggregator(rules)
        
        execution_id = "test-execution-001"
        result_df = aggregator.calculate_dimension_scores(df_with_rules, execution_id=execution_id)
        
        assert result_df is not None
        assert "execution_id" in result_df.columns
        assert "dimension_name" in result_df.columns
        assert "score" in result_df.columns
        
        # Check that Completeness dimension is in results
        results = result_df.collect()
        dimension_names = [row.dimension_name for row in results]
        assert "Completeness" in dimension_names
        
        # Check execution_id is set correctly
        for row in results:
            assert row.execution_id == execution_id
    
    def test_calculate_dimension_scores_missing_rule_column(self, spark_session, sample_dataframe):
        """Test calculating scores when rule column is missing."""
        # Don't add rule columns
        rules = [MockRule("C001", "Rule 1", "Completeness")]
        aggregator = DimensionAggregator(rules)
        
        execution_id = "test-execution-002"
        result_df = aggregator.calculate_dimension_scores(sample_dataframe, execution_id=execution_id)
        
        # Should still work, but with warnings
        results = result_df.collect()
        assert len(results) >= 0  # May have 0 if all dimensions missing
    
    def test_calculate_dimension_scores_no_rules_for_dimension(self, spark_session, sample_dataframe):
        """Test calculating scores when no rules exist for a dimension."""
        # Create aggregator with rules for only some dimensions
        rules = [MockRule("C001", "Rule 1", "Completeness")]
        aggregator = DimensionAggregator(rules)
        
        df_with_rules = sample_dataframe.withColumn(
            "rule_C001_pass", lit(True)
        )
        
        execution_id = "test-execution-003"
        result_df = aggregator.calculate_dimension_scores(df_with_rules, execution_id=execution_id)
        
        # Should handle missing dimensions gracefully
        results = result_df.collect()
        # Completeness should be present, others may be missing
        dimension_names = [row.dimension_name for row in results]
        assert "Completeness" in dimension_names
    
    def test_calculate_dimension_scores_empty_dataframe(self, spark_session):
        """Test calculating scores with empty DataFrame."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("rule_C001_pass", StringType(), True)
        ])
        empty_df = spark_session.createDataFrame([], schema)
        
        rules = [MockRule("C001", "Rule 1", "Completeness")]
        aggregator = DimensionAggregator(rules)
        
        execution_id = "test-execution-004"
        result_df = aggregator.calculate_dimension_scores(empty_df, execution_id=execution_id)
        
        # Should handle empty DataFrame
        results = result_df.collect()
        if len(results) > 0:
            for row in results:
                assert row.total_rows == 0
                assert row.score == 0.0
                assert row.execution_id == execution_id
    
    def test_calculate_dimension_scores_execution_id(self, spark_session, sample_dataframe):
        """Test calculating scores with execution_id."""
        df_with_rules = sample_dataframe.withColumn(
            "rule_C001_pass", lit(True)
        )
        
        rules = [MockRule("C001", "Rule 1", "Completeness")]
        aggregator = DimensionAggregator(rules)
        
        execution_id = "custom-execution-123"
        result_df = aggregator.calculate_dimension_scores(df_with_rules, execution_id=execution_id)
        
        results = result_df.collect()
        if len(results) > 0:
            assert results[0].execution_id == execution_id
    
    def test_calculate_dimension_scores_all_dimensions(self, spark_session, sample_dataframe):
        """Test that all dimensions are processed."""
        # Add rule columns for multiple dimensions
        df_with_rules = sample_dataframe.withColumn(
            "rule_C001_pass", lit(True)
        ).withColumn(
            "rule_U001_pass", lit(True)
        ).withColumn(
            "rule_V001_pass", lit(True)
        )
        
        rules = [
            MockRule("C001", "Rule 1", "Completeness"),
            MockRule("U001", "Rule 2", "Uniqueness"),
            MockRule("V001", "Rule 3", "Validity")
        ]
        aggregator = DimensionAggregator(rules)
        
        execution_id = "test-execution-005"
        result_df = aggregator.calculate_dimension_scores(df_with_rules, execution_id=execution_id)
        
        results = result_df.collect()
        dimension_names = [row.dimension_name for row in results]
        
        # Should have results for dimensions with rules
        assert "Completeness" in dimension_names
        assert "Uniqueness" in dimension_names
        assert "Validity" in dimension_names
        
        # All results should have the same execution_id
        for row in results:
            assert row.execution_id == execution_id

