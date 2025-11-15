from pathlib import Path
from typing import Optional
from datetime import datetime
from uuid import uuid4
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from src.rules.completeness_rules import get_completeness_rules
from src.rules.uniqueness_rules import get_uniqueness_rules
from src.rules.validity_rules import get_validity_rules
from src.rules.conformity_rules import get_conformity_rules
from src.rules.consistency_rules import get_consistency_rules
from src.rules.timeliness_rules import get_timeliness_rules
from src.evaluator import RuleEvaluator
from src.dimension_aggregator import DimensionAggregator
from src.config.config_loader import load_config
from src.utils.spark_session import create_spark_session, stop_spark_session
import logging

logger = logging.getLogger(__name__)


class DQOrchestrator:
    def __init__(self, config_path: Optional[str] = None):
        self.config = load_config(config_path)
        self.spark = None
        self.rules = []
        self.evaluator = None
        self.aggregator = None
        
    def setup(self):
        # Create Spark session
        spark_config = self.config.get("spark", {})
        self.spark = create_spark_session(
            app_name=spark_config.get("app_name", "DataQualityMonitoring"),
            master=spark_config.get("master", "local[*]")
        )
        logger.info("Spark session created")
        
        # Load all rules
        self.rules = []
        self.rules.extend(get_completeness_rules())
        self.rules.extend(get_uniqueness_rules())
        self.rules.extend(get_validity_rules())
        self.rules.extend(get_conformity_rules())
        self.rules.extend(get_consistency_rules())
        
        # Timeliness rules need config
        timeliness_config = self.config.get("timeliness", {})
        self.rules.extend(get_timeliness_rules(
            sla_days=timeliness_config.get("sla_days", 30),
            max_age_years=timeliness_config.get("max_age_years", 5)
        ))
        
        logger.info(f"Loaded {len(self.rules)} rules")
        
        # Initialize evaluator and aggregator
        self.evaluator = RuleEvaluator(self.rules)
        self.aggregator = DimensionAggregator(self.rules)
    
    def load_data(self, data_path: str) -> DataFrame:
        logger.info(f"Loading data from {data_path}")
        df = self.spark.read.csv(
            data_path,
            header=True,
            inferSchema=True
        )
        logger.info(f"Loaded {df.count()} rows")
        return df
    
    def run_analysis(self, data_path: str, output_dir: str = "output") -> dict:
        """
        Run data quality analysis and generate outputs matching the data model.
        
        Returns:
            dict with keys: execution_id, execution_df, dimension_scores_df, rule_results_df
        """
        execution_id = str(uuid4())
        execution_timestamp = datetime.now()
        execution_status = "SUCCESS"
        total_rows = 0
        dataset_name = self.config.get("dataset", {}).get("name", "grant_applications")
        dataset_version = self.config.get("dataset", {}).get("version", None)
        
        try:
            # Load data
            df = self.load_data(data_path)
            total_rows = df.count()
            
            # Evaluate all rules
            logger.info("Evaluating all rules...")
            df_with_rules = self.evaluator.evaluate_all(df)
            
            # Calculate dimension scores
            logger.info("Calculating dimension scores...")
            dimension_scores_df = self.aggregator.calculate_dimension_scores(
                df_with_rules,
                execution_id=execution_id
            )
            
            # Generate rule results
            logger.info("Generating rule results...")
            rule_results_df = self.evaluator.get_rule_results_df(
                df_with_rules,
                execution_id=execution_id
            )
            
            # Create execution record
            execution_df = self._create_execution_df(
                execution_id=execution_id,
                execution_timestamp=execution_timestamp,
                dataset_name=dataset_name,
                dataset_version=dataset_version,
                total_rows=total_rows,
                status=execution_status
            )
            
            # Save results
            output_path = Path(output_dir)
            output_path.mkdir(exist_ok=True)
            
            # Save dq_execution (one row)
            execution_dir = output_path / "dq_execution"
            logger.info(f"Saving execution record to {execution_dir}")
            execution_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(execution_dir))
            
            # Save dq_dimension_scores (multiple rows, one per dimension)
            scores_dir = output_path / "dq_dimension_scores"
            logger.info(f"Saving dimension scores to {scores_dir}")
            dimension_scores_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(scores_dir))
            
            # Save minimal output (dimension_name, score) as required by deliverables
            scores_minimal_dir = output_path / "dq_dimension_scores_minimal"
            logger.info(f"Saving minimal dimension scores to {scores_minimal_dir}")
            dimension_scores_df.select("dimension_name", "score").coalesce(1).write.mode("overwrite").option("header", "true").csv(str(scores_minimal_dir))
            
            # Save dq_rule_results (multiple rows, one per rule)
            rule_results_dir = output_path / "dq_rule_results"
            logger.info(f"Saving rule results to {rule_results_dir}")
            rule_results_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(rule_results_dir))
            
            # Also save legacy full results for backward compatibility
            full_dir = output_path / "dq_dimension_scores_full"
            logger.info(f"Saving legacy full results to {full_dir}")
            # Create legacy format with all fields flattened
            legacy_df = dimension_scores_df.withColumn("execution_timestamp", lit(execution_timestamp.isoformat())) \
                                          .withColumn("dataset_name", lit(dataset_name))
            legacy_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(full_dir))
            
            logger.info("Analysis completed successfully")
            return {
                "execution_id": execution_id,
                "execution_df": execution_df,
                "dimension_scores_df": dimension_scores_df,
                "rule_results_df": rule_results_df
            }
            
        except Exception as e:
            execution_status = "FAILED"
            logger.error(f"Error during analysis: {str(e)}", exc_info=True)
            
            # Still create execution record with FAILED status
            try:
                execution_df = self._create_execution_df(
                    execution_id=execution_id,
                    execution_timestamp=execution_timestamp,
                    dataset_name=dataset_name,
                    dataset_version=dataset_version,
                    total_rows=total_rows,
                    status=execution_status
                )
                output_path = Path(output_dir)
                output_path.mkdir(exist_ok=True)
                execution_dir = output_path / "dq_execution"
                execution_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(execution_dir))
            except Exception as save_error:
                logger.error(f"Failed to save execution record: {str(save_error)}")
            
            raise
    
    def _create_execution_df(self, execution_id: str, execution_timestamp: datetime,
                            dataset_name: str, dataset_version: Optional[str],
                            total_rows: int, status: str) -> DataFrame:
        """
        Create DataFrame matching dq_execution table structure.
        
        Args:
            execution_id: Unique identifier for this execution
            execution_timestamp: When the analysis was run
            dataset_name: Name of dataset analyzed
            dataset_version: Version/identifier of dataset (optional)
            total_rows: Total number of rows analyzed
            status: Execution status (SUCCESS/FAILED)
            
        Returns:
            DataFrame with columns: execution_id, execution_timestamp, dataset_name,
            dataset_version, total_rows, status
        """
        execution_data = [{
            "execution_id": execution_id,
            "execution_timestamp": execution_timestamp,
            "dataset_name": dataset_name,
            "dataset_version": dataset_version,
            "total_rows": total_rows,
            "status": status
        }]
        
        schema = StructType([
            StructField("execution_id", StringType(), False),
            StructField("execution_timestamp", TimestampType(), False),
            StructField("dataset_name", StringType(), False),
            StructField("dataset_version", StringType(), True),
            StructField("total_rows", IntegerType(), False),
            StructField("status", StringType(), False)
        ])
        
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active Spark session found")
        
        execution_df = spark.createDataFrame(execution_data, schema=schema)
        
        return execution_df
    
    def cleanup(self):
        if self.spark:
            stop_spark_session(self.spark)

