"""Unit tests for rule evaluator."""

import pytest
from unittest.mock import patch
from pyspark.sql.functions import col, lit
from src.evaluator import RuleEvaluator
from src.rules.base_rule import BaseRule


class MockRule(BaseRule):
    """Mock rule for testing."""
    
    def __init__(self, rule_id, rule_name, dimension, column_name):
        super().__init__(rule_id, rule_name, dimension)
        self.column_name = column_name
    
    def evaluate(self, df):
        return col(self.column_name).isNotNull()


class TestRuleEvaluator:
    """Test cases for rule evaluator."""
    
    def test_init(self, spark_session):
        """Test RuleEvaluator initialization."""
        rules = [MockRule("R001", "Test Rule", "Test", "application_id")]
        evaluator = RuleEvaluator(rules)
        
        assert evaluator.rules == rules
        assert len(evaluator.rules) == 1
    
    def test_evaluate_all_success(self, spark_session, sample_dataframe):
        """Test evaluating all rules successfully."""
        rules = [
            MockRule("R001", "Rule 1", "Test", "application_id"),
            MockRule("R002", "Rule 2", "Test", "first_name")
        ]
        evaluator = RuleEvaluator(rules)
        
        result_df = evaluator.evaluate_all(sample_dataframe)
        
        assert "rule_R001_pass" in result_df.columns
        assert "rule_R002_pass" in result_df.columns
        
        # Check that rule columns were added
        rows = result_df.collect()
        assert len(rows) == 3
    
    def test_evaluate_all_with_exception(self, spark_session, sample_dataframe):
        """Test evaluating rules when one raises an exception."""
        class FailingRule(BaseRule):
            def __init__(self):
                super().__init__("FAIL", "Failing Rule", "Test")
            
            def evaluate(self, df):
                raise ValueError("Rule evaluation failed")
        
        rules = [
            MockRule("R001", "Rule 1", "Test", "application_id"),
            FailingRule()
        ]
        evaluator = RuleEvaluator(rules)
        
        result_df = evaluator.evaluate_all(sample_dataframe)
        
        # Both rules should have columns
        assert "rule_R001_pass" in result_df.columns
        assert "rule_FAIL_pass" in result_df.columns
        
        # Failing rule should mark all as False
        fail_results = result_df.select("rule_FAIL_pass").collect()
        assert all(row.rule_FAIL_pass == False for row in fail_results)
    
    def test_get_rule_results(self, spark_session, sample_dataframe):
        """Test getting rule results statistics."""
        rules = [
            MockRule("R001", "Rule 1", "Test", "application_id"),
            MockRule("R002", "Rule 2", "Test", "first_name")
        ]
        evaluator = RuleEvaluator(rules)
        
        result_df = evaluator.evaluate_all(sample_dataframe)
        results = evaluator.get_rule_results(result_df)
        
        assert "R001" in results
        assert "R002" in results
        
        r001_result = results["R001"]
        assert r001_result["rule_id"] == "R001"
        assert r001_result["rule_name"] == "Rule 1"
        assert r001_result["dimension"] == "Test"
        assert r001_result["total_rows"] == 3
        assert "passing_rows" in r001_result
        assert "failing_rows" in r001_result
        assert "pass_rate" in r001_result
        assert 0 <= r001_result["pass_rate"] <= 100
    
    def test_get_rule_results_missing_column(self, spark_session, sample_dataframe):
        """Test get_rule_results when rule column is missing."""
        rules = [MockRule("R001", "Rule 1", "Test", "application_id")]
        evaluator = RuleEvaluator(rules)
        
        # Don't evaluate, so column won't exist
        results = evaluator.get_rule_results(sample_dataframe)
        
        # Should not have R001 in results since column doesn't exist
        assert "R001" not in results
    
    def test_get_rule_results_empty_dataframe(self, spark_session):
        """Test get_rule_results with empty DataFrame."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True)
        ])
        empty_df = spark_session.createDataFrame([], schema)
        
        rules = [MockRule("R001", "Rule 1", "Test", "application_id")]
        evaluator = RuleEvaluator(rules)
        
        result_df = evaluator.evaluate_all(empty_df)
        results = evaluator.get_rule_results(result_df)
        
        if "R001" in results:
            assert results["R001"]["total_rows"] == 0
            assert results["R001"]["pass_rate"] == 0.0
    
    def test_evaluate_all_empty_rules(self, spark_session, sample_dataframe):
        """Test evaluating with empty rules list."""
        evaluator = RuleEvaluator([])
        result_df = evaluator.evaluate_all(sample_dataframe)
        
        # Should return original DataFrame unchanged
        assert result_df.columns == sample_dataframe.columns
    
    def test_get_rule_results_df_success(self, spark_session, sample_dataframe):
        """Test get_rule_results_df generates correct DataFrame."""
        rules = [
            MockRule("R001", "Rule 1", "Test", "application_id"),
            MockRule("R002", "Rule 2", "Test", "first_name")
        ]
        evaluator = RuleEvaluator(rules)
        
        result_df = evaluator.evaluate_all(sample_dataframe)
        rule_results_df = evaluator.get_rule_results_df(result_df, "exec-123")
        
        assert rule_results_df.count() == 2
        assert "execution_id" in rule_results_df.columns
        assert "rule_id" in rule_results_df.columns
        assert "dimension_name" in rule_results_df.columns
        assert "rule_name" in rule_results_df.columns
        assert "total_rows" in rule_results_df.columns
        assert "passing_rows" in rule_results_df.columns
        assert "failing_rows" in rule_results_df.columns
        assert "failure_rate" in rule_results_df.columns
        
        # Check execution_id is set correctly
        rows = rule_results_df.collect()
        assert all(row.execution_id == "exec-123" for row in rows)
    
    def test_get_rule_results_df_missing_column(self, spark_session, sample_dataframe):
        """Test get_rule_results_df when rule column is missing."""
        rules = [MockRule("R001", "Rule 1", "Test", "application_id")]
        evaluator = RuleEvaluator(rules)
        
        # Don't evaluate, so column won't exist
        rule_results_df = evaluator.get_rule_results_df(sample_dataframe, "exec-123")
        
        # Should still create row but with all failing
        assert rule_results_df.count() == 1
        row = rule_results_df.collect()[0]
        assert row.rule_id == "R001"
        assert row.failing_rows == sample_dataframe.count()
        assert row.failure_rate == 100.0
    
    def test_get_rule_results_df_empty_dataframe(self, spark_session):
        """Test get_rule_results_df with empty DataFrame."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True)
        ])
        empty_df = spark_session.createDataFrame([], schema)
        
        rules = [MockRule("R001", "Rule 1", "Test", "application_id")]
        evaluator = RuleEvaluator(rules)
        
        result_df = evaluator.evaluate_all(empty_df)
        rule_results_df = evaluator.get_rule_results_df(result_df, "exec-123")
        
        assert rule_results_df.count() == 1
        row = rule_results_df.collect()[0]
        assert row.total_rows == 0
        assert row.failure_rate == 0.0
    
    def test_get_rule_results_df_no_spark_session(self, spark_session, sample_dataframe):
        """Test get_rule_results_df raises error when no Spark session."""
        from pyspark.sql import SparkSession
        
        rules = [MockRule("R001", "Rule 1", "Test", "application_id")]
        evaluator = RuleEvaluator(rules)
        
        result_df = evaluator.evaluate_all(sample_dataframe)
        
        # Collect the data before stopping Spark, as the DataFrame needs Spark to exist
        # We'll test the error by temporarily removing the active session
        # Actually, we can't easily test this without breaking the session, so let's
        # test with a mock that simulates no active session
        with patch.object(SparkSession, 'getActiveSession', return_value=None):
            with pytest.raises(RuntimeError, match="No active Spark session found"):
                evaluator.get_rule_results_df(result_df, "exec-123")

