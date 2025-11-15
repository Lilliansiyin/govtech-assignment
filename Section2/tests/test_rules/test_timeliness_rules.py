"""Unit tests for timeliness rules."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql.functions import col
from src.rules.timeliness_rules import (
    T001_DecisionLagWithinSLA,
    T002_DataFreshness,
    get_timeliness_rules
)


class TestT001_DecisionLagWithinSLA:
    """Test cases for T001_DecisionLagWithinSLA rule."""
    
    def test_init_default_sla(self):
        """Test T001 initialization with default SLA."""
        rule = T001_DecisionLagWithinSLA()
        
        assert rule.rule_id == "T001"
        assert rule.dimension == "Timeliness"
        assert rule.sla_days == 30
        assert "30 days" in rule.rule_name
    
    def test_init_custom_sla(self):
        """Test T001 initialization with custom SLA."""
        rule = T001_DecisionLagWithinSLA(sla_days=60)
        
        assert rule.sla_days == 60
        assert "60 days" in rule.rule_name
    
    def test_evaluate_within_sla(self, spark_session):
        """Test T001 evaluation with decision lag within SLA."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True),
            StructField("decision_date", StringType(), True),
            StructField("application_status", StringType(), True)
        ])
        
        data = [
            ("APP001", "2024-01-01", "2024-01-15", "Approved"),  # 14 days
            ("APP002", "2024-01-01", "2024-01-30", "Rejected"),  # 29 days
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T001_DecisionLagWithinSLA(sla_days=30)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T001_pass", result_col)
        results = result_df.select("rule_T001_pass").collect()
        
        assert all(row.rule_T001_pass == True for row in results)
    
    def test_evaluate_exceeds_sla(self, spark_session):
        """Test T001 evaluation with decision lag exceeding SLA."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True),
            StructField("decision_date", StringType(), True),
            StructField("application_status", StringType(), True)
        ])
        
        data = [
            ("APP001", "2024-01-01", "2024-02-15", "Approved"),  # 45 days
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T001_DecisionLagWithinSLA(sla_days=30)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T001_pass", result_col)
        results = result_df.select("rule_T001_pass").collect()
        
        assert all(row.rule_T001_pass == False for row in results)
    
    def test_evaluate_pending_status(self, spark_session):
        """Test T001 evaluation with pending status (should pass)."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True),
            StructField("decision_date", StringType(), True),
            StructField("application_status", StringType(), True)
        ])
        
        data = [
            ("APP001", "2024-01-01", None, "Pending"),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T001_DecisionLagWithinSLA(sla_days=30)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T001_pass", result_col)
        results = result_df.select("rule_T001_pass").collect()
        
        assert all(row.rule_T001_pass == True for row in results)
    
    def test_evaluate_missing_dates(self, spark_session):
        """Test T001 evaluation with missing dates."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True),
            StructField("decision_date", StringType(), True),
            StructField("application_status", StringType(), True)
        ])
        
        data = [
            ("APP001", None, "2024-01-15", "Approved"),
            ("APP002", "2024-01-01", None, "Approved"),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T001_DecisionLagWithinSLA(sla_days=30)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T001_pass", result_col)
        results = result_df.select("rule_T001_pass").collect()
        
        assert all(row.rule_T001_pass == True for row in results)
    
    def test_evaluate_invalid_date_format(self, spark_session):
        """Test T001 evaluation with invalid date format."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True),
            StructField("decision_date", StringType(), True),
            StructField("application_status", StringType(), True)
        ])
        
        data = [
            ("APP001", "invalid-date", "2024-01-15", "Approved"),
            ("APP002", "2024-01-01", "invalid-date", "Approved"),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T001_DecisionLagWithinSLA(sla_days=30)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T001_pass", result_col)
        results = result_df.select("rule_T001_pass").collect()
        
        # Should fail when dates can't be parsed
        assert all(row.rule_T001_pass == False for row in results)
    
    def test_evaluate_negative_days(self, spark_session):
        """Test T001 evaluation with negative days (decision before application)."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True),
            StructField("decision_date", StringType(), True),
            StructField("application_status", StringType(), True)
        ])
        
        data = [
            ("APP001", "2024-01-15", "2024-01-01", "Approved"),  # Decision before application
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T001_DecisionLagWithinSLA(sla_days=30)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T001_pass", result_col)
        results = result_df.select("rule_T001_pass").collect()
        
        # Should fail when days_diff < 0
        assert all(row.rule_T001_pass == False for row in results)


class TestT002_DataFreshness:
    """Test cases for T002_DataFreshness rule."""
    
    def test_init_default_max_age(self):
        """Test T002 initialization with default max age."""
        rule = T002_DataFreshness()
        
        assert rule.rule_id == "T002"
        assert rule.dimension == "Timeliness"
        assert rule.max_age_days == 5 * 365
        assert "5 years old" in rule.rule_name
    
    def test_init_custom_max_age(self):
        """Test T002 initialization with custom max age."""
        rule = T002_DataFreshness(max_age_years=10)
        
        assert rule.max_age_days == 10 * 365
        assert "10 years old" in rule.rule_name
    
    def test_evaluate_fresh_data(self, spark_session):
        """Test T002 evaluation with fresh data."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True)
        ])
        
        # Data from 1 year ago
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        data = [
            ("APP001", one_year_ago),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T002_DataFreshness(max_age_years=5)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T002_pass", result_col)
        results = result_df.select("rule_T002_pass").collect()
        
        assert all(row.rule_T002_pass == True for row in results)
    
    def test_evaluate_old_data(self, spark_session):
        """Test T002 evaluation with old data."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True)
        ])
        
        # Data from 6 years ago
        six_years_ago = (datetime.now() - timedelta(days=6*365)).strftime("%Y-%m-%d")
        data = [
            ("APP001", six_years_ago),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T002_DataFreshness(max_age_years=5)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T002_pass", result_col)
        results = result_df.select("rule_T002_pass").collect()
        
        assert all(row.rule_T002_pass == False for row in results)
    
    def test_evaluate_missing_date(self, spark_session):
        """Test T002 evaluation with missing application_date."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True)
        ])
        
        data = [
            ("APP001", None),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T002_DataFreshness(max_age_years=5)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T002_pass", result_col)
        results = result_df.select("rule_T002_pass").collect()
        
        assert all(row.rule_T002_pass == True for row in results)
    
    def test_evaluate_invalid_date_format(self, spark_session):
        """Test T002 evaluation with invalid date format."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True)
        ])
        
        data = [
            ("APP001", "invalid-date"),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T002_DataFreshness(max_age_years=5)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T002_pass", result_col)
        results = result_df.select("rule_T002_pass").collect()
        
        # Should fail when date can't be parsed
        assert all(row.rule_T002_pass == False for row in results)
    
    def test_evaluate_at_boundary(self, spark_session):
        """Test T002 evaluation at boundary (exactly max_age_years)."""
        from pyspark.sql.types import StructType, StructField, StringType
        
        schema = StructType([
            StructField("application_id", StringType(), True),
            StructField("application_date", StringType(), True)
        ])
        
        # Data from exactly 5 years ago
        five_years_ago = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
        data = [
            ("APP001", five_years_ago),
        ]
        
        df = spark_session.createDataFrame(data, schema)
        rule = T002_DataFreshness(max_age_years=5)
        result_col = rule.evaluate(df)
        
        result_df = df.withColumn("rule_T002_pass", result_col)
        results = result_df.select("rule_T002_pass").collect()
        
        # Should pass (<= max_age_days)
        assert all(row.rule_T002_pass == True for row in results)


class TestGetTimelinessRules:
    """Test cases for get_timeliness_rules function."""
    
    def test_get_timeliness_rules_default(self):
        """Test get_timeliness_rules with default parameters."""
        rules = get_timeliness_rules()
        
        assert len(rules) == 2
        assert isinstance(rules[0], T001_DecisionLagWithinSLA)
        assert isinstance(rules[1], T002_DataFreshness)
        assert rules[0].sla_days == 30
        assert rules[1].max_age_days == 5 * 365
    
    def test_get_timeliness_rules_custom(self):
        """Test get_timeliness_rules with custom parameters."""
        rules = get_timeliness_rules(sla_days=60, max_age_years=10)
        
        assert len(rules) == 2
        assert rules[0].sla_days == 60
        assert rules[1].max_age_days == 10 * 365

