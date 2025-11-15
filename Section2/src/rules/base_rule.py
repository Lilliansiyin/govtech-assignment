from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.column import Column


class BaseRule(ABC):
    def __init__(self, rule_id: str, rule_name: str, dimension: str):
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.dimension = dimension
    
    @abstractmethod
    def evaluate(self, df: DataFrame) -> Column:
        """
        Evaluate the rule against a DataFrame.
        
        Args:
            df: Input DataFrame
        
        Returns:
            Boolean Column indicating pass (True) or fail (False) for each row
        """
        pass
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(rule_id={self.rule_id}, dimension={self.dimension})"

