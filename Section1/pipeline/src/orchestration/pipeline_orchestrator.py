import os
from typing import Dict

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from src.bronze.csv_ingester import CSVIngester
from src.gold.dimensional_modeler import DimensionalModeler
from src.silver.business_rules import BusinessRules
from src.silver.cleaner import DataCleaner
from src.silver.quality_scorer import QualityScorer
from src.silver.validators import Validator
from src.utils.logger import StructuredLogger
from src.utils.spark_session import create_spark_session, is_glue_environment


class PipelineOrchestrator:
    def __init__(
        self, config_path: str, validation_rules_path: str, postal_mapping_path: str
    ):
        self.config = self._load_config(config_path)
        self.validation_config = self._load_config(validation_rules_path)
        self.postal_mapping_path = postal_mapping_path

        merged_config = {**self.config, **self.validation_config}

        self.logger = StructuredLogger("PipelineOrchestrator")
        self.spark = create_spark_session("tax-pipeline")

        self.bronze_ingester = CSVIngester(self.spark, self.config, self.logger)
        self.validator = Validator(merged_config, self.logger)
        self.business_rules = BusinessRules(merged_config, self.logger)
        self.cleaner = DataCleaner(merged_config, self.logger)
        self.quality_scorer = QualityScorer(merged_config, self.logger)
        self.dimensional_modeler = DimensionalModeler(
            self.spark, self.config, self.logger
        )

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

    def _get_storage_path(self, path_key: str) -> str:
        """Get storage path, preferring cloud paths if available, otherwise local paths."""
        storage_config = self.config.get("storage", {})
        self.logger.info(f"Getting storage path for {path_key}. Storage config keys: {list(storage_config.keys())}")
        
        # In Glue environment, use cloud paths; locally, use local paths
        if is_glue_environment():
            self.logger.info("Running in Glue environment, using cloud paths")
            cloud_config = storage_config.get("cloud", {})
            self.logger.info(f"Cloud config keys: {list(cloud_config.keys())}")
            cloud_path = cloud_config.get(path_key)
            self.logger.info(f"Raw cloud_path for {path_key}: '{cloud_path}' (type: {type(cloud_path)})")
            
            if cloud_path and cloud_path.strip():
                self.logger.info(f"Using cloud storage path for {path_key}: {cloud_path}")
                return cloud_path
            else:
                raise ValueError(
                    f"Running in Glue environment but cloud path for {path_key} is not configured or is empty. "
                    f"Cloud config: {cloud_config}. Please set storage.cloud.{path_key} in pipeline_config.yaml"
                )
        
        local_path = storage_config.get("local", {}).get(path_key, f"./data/{path_key.replace('_path', '')}")
        if not local_path or not local_path.strip():
            raise ValueError(f"Local path for {path_key} is empty")
        self.logger.info(f"Using local storage path for {path_key}: {local_path}")
        # Only create directory for local paths (not needed for S3)
        if not local_path.startswith("s3"):
            os.makedirs(local_path, exist_ok=True)
        return local_path

    def run_bronze_layer(self, csv_path: str, source_file: str = None) -> DataFrame:
        self.logger.info("Starting Bronze layer processing")

        df = self.bronze_ingester.ingest(csv_path, source_file)

        bronze_path = self._get_storage_path("bronze_path")
        self.logger.info(f"Retrieved bronze_path: '{bronze_path}' (length: {len(bronze_path) if bronze_path else 0})")
        
        if not bronze_path or not bronze_path.strip():
            raise ValueError(f"Bronze path is empty! Config: {self.config.get('storage', {})}")

        self.bronze_ingester.write_bronze(df, bronze_path)

        self.logger.info("Bronze layer processing completed")
        return df

    def run_silver_layer(self, df: DataFrame) -> tuple:
        self.logger.info("Starting Silver layer processing")

        df = self.validator.apply_all_validations(df)
        df = self.business_rules.apply_cpf_correction(df)
        df = self.cleaner.clean_all(df)
        df = self.quality_scorer.score_and_classify(df)

        accept_quarantine = df.filter(
            (col("dq_classification") == "Accept")
            | (col("dq_classification") == "Quarantine")
        )

        rejected = df.filter(col("dq_classification") == "Reject")

        silver_path = self._get_storage_path("silver_path")
        rejected_path = self._get_storage_path("rejected_path")

        # Normalize S3 paths - ensure they have path components (not just bucket root)
        # Spark requires a path within the bucket, not just the bucket root
        def ensure_path_component(path: str, default_name: str) -> str:
            if path.startswith("s3://") or path.startswith("s3a://"):
                path = path.rstrip("/")
                parts = path.replace("s3://", "").replace("s3a://", "").split("/", 1)
                if len(parts) == 1 or (len(parts) == 2 and not parts[1].strip()):
                    bucket_name = parts[0]
                    protocol = "s3://" if path.startswith("s3://") else "s3a://"
                    path = f"{protocol}{bucket_name}/{default_name}"
                    self.logger.info(f"Path was bucket root only, added default path component: {path}")
            return path
        
        silver_path = ensure_path_component(silver_path, "silver")
        rejected_path = ensure_path_component(rejected_path, "rejected")

        initial_accept_count = accept_quarantine.count()
        initial_rejected_count = rejected.count()
        
        null_accept_count = accept_quarantine.filter(col("assessment_year").isNull()).count()
        null_rejected_count = rejected.filter(col("assessment_year").isNull()).count()
        
        if null_accept_count > 0:
            self.logger.warning(
                f"Found {null_accept_count} records with null assessment_year in accept/quarantine out of {initial_accept_count} total. Filtering them out."
            )
            accept_quarantine = accept_quarantine.filter(col("assessment_year").isNotNull())
        
        if null_rejected_count > 0:
            self.logger.warning(
                f"Found {null_rejected_count} records with null assessment_year in rejected out of {initial_rejected_count} total. Filtering them out."
            )
            rejected = rejected.filter(col("assessment_year").isNotNull())
        
        accept_quarantine_count = accept_quarantine.count()
        if accept_quarantine_count > 0:
            accept_quarantine.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .partitionBy("assessment_year") \
                .parquet(silver_path)
            self.logger.info(f"Wrote {accept_quarantine_count} records to Silver layer")
        else:
            self.logger.warning("No records to write to Silver layer")

        rejected_count = rejected.count()
        if rejected_count > 0:
            rejected.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .partitionBy("assessment_year") \
                .parquet(rejected_path)
            self.logger.info(f"Wrote {rejected_count} rejected records")

        self.logger.info("Silver layer processing completed")
        return accept_quarantine, rejected

    def run_gold_layer(self, df: DataFrame):
        self.logger.info("Starting Gold layer processing")

        star_schema = self.dimensional_modeler.build_star_schema(
            df, self.postal_mapping_path
        )

        gold_path = self._get_storage_path("gold_path")

        self.dimensional_modeler.write_gold_layer(star_schema, gold_path)

        self.logger.info("Gold layer processing completed")
        return star_schema

    def run_full_pipeline(self, csv_path: str, source_file: str = None):
        self.logger.info("Starting full pipeline execution")

        try:
            bronze_df = self.run_bronze_layer(csv_path, source_file)
            silver_df, rejected_df = self.run_silver_layer(bronze_df)
            star_schema = self.run_gold_layer(silver_df)

            self.logger.info("Pipeline execution completed successfully")
            return {
                "bronze_count": bronze_df.count(),
                "silver_count": silver_df.count(),
                "rejected_count": rejected_df.count(),
                "gold_tables": list(star_schema.keys()),
            }
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}", error=str(e))
            raise
        finally:
            self.spark.stop()
