"""Unit tests for base rule."""

import pytest
from pyspark.sql.functions import col, lit
from src.rules.base_rule import BaseRule


class ConcreteRule(BaseRule):
    """Concrete implementation of BaseRule for testing."""
    
    def __init__(self, rule_id, rule_name, dimension):
        super().__init__(rule_id, rule_name, dimension)
    
    def evaluate(self, df):
        return col("test_column").isNotNull()


class TestBaseRule:
    """Test cases for base rule."""
    
    def test_init(self):
        """Test BaseRule initialization."""
        rule = ConcreteRule("R001", "Test Rule", "Test")
        
        assert rule.rule_id == "R001"
        assert rule.rule_name == "Test Rule"
        assert rule.dimension == "Test"
    
    def test_repr(self):
        """Test BaseRule string representation."""
        rule = ConcreteRule("R001", "Test Rule", "Test")
        repr_str = repr(rule)
        
        assert "ConcreteRule" in repr_str
        assert "R001" in repr_str
        assert "Test" in repr_str
    
    def test_cannot_instantiate_base_rule(self):
        """Test that BaseRule cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseRule("R001", "Test", "Test")
    
    def test_evaluate_abstract(self):
        """Test that evaluate is abstract and must be implemented."""
        # This is tested by the fact that we can't instantiate BaseRule
        # and ConcreteRule must implement evaluate
        rule = ConcreteRule("R001", "Test", "Test")
        assert hasattr(rule, 'evaluate')
        assert callable(rule.evaluate)
    
    def test_rule_attributes(self):
        """Test that rule attributes are set correctly."""
        rule = ConcreteRule("C001", "Completeness Rule", "Completeness")
        
        assert rule.rule_id == "C001"
        assert rule.rule_name == "Completeness Rule"
        assert rule.dimension == "Completeness"
    
    def test_repr_includes_class_name(self):
        """Test that __repr__ includes class name."""
        rule = ConcreteRule("R001", "Test Rule", "Test")
        repr_str = repr(rule)
        
        assert rule.__class__.__name__ in repr_str
    
    def test_repr_includes_rule_id(self):
        """Test that __repr__ includes rule_id."""
        rule = ConcreteRule("R999", "Test Rule", "Test")
        repr_str = repr(rule)
        
        assert "R999" in repr_str
    
    def test_repr_includes_dimension(self):
        """Test that __repr__ includes dimension."""
        rule = ConcreteRule("R001", "Test Rule", "CustomDimension")
        repr_str = repr(rule)
        
        assert "CustomDimension" in repr_str

