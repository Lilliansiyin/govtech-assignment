from datetime import datetime
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import hash as spark_hash
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.utils.logger import StructuredLogger


class CSVIngester:
    def __init__(self, spark: SparkSession, config: Dict, logger: StructuredLogger):
        self.spark = spark
        self.config = config
        self.logger = logger
        self.required_columns = config.get("ingestion", {}).get("required_columns", [])

    def _get_schema(self) -> StructType:
        return StructType(
            [
                StructField("taxpayer_id", StringType(), True),
                StructField("nric", StringType(), True),
                StructField("full_name", StringType(), True),
                StructField("filing_status", StringType(), True),
                StructField("assessment_year", IntegerType(), True),
                StructField("filing_date", StringType(), True),
                StructField("annual_income_sgd", DoubleType(), True),
                StructField("chargeable_income_sgd", DoubleType(), True),
                StructField("tax_payable_sgd", DoubleType(), True),
                StructField("tax_paid_sgd", DoubleType(), True),
                StructField("total_reliefs_sgd", DoubleType(), True),
                StructField("number_of_dependents", IntegerType(), True),
                StructField("occupation", StringType(), True),
                StructField("residential_status", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("housing_type", StringType(), True),
                StructField("cpf_contributions_sgd", DoubleType(), True),
                StructField("foreign_income_sgd", DoubleType(), True),
            ]
        )

    def _validate_columns(self, df: DataFrame, source_file: str) -> DataFrame:
        existing_columns = set(df.columns)
        required_columns = set(self.required_columns)
        missing_columns = required_columns - existing_columns

        if missing_columns:
            self.logger.warning(
                f"Missing required columns in {source_file}",
                missing_columns=list(missing_columns),
            )

        return df

    def ingest(self, csv_path: str, source_file: str = None) -> DataFrame:
        if source_file is None:
            source_file = csv_path

        self.logger.info(f"Ingesting CSV file: {csv_path}")

        schema = self._get_schema()
        df = (
            self.spark.read.option("header", "true")
            .option("inferSchema", "false")
            .schema(schema)
            .csv(csv_path)
        )

        df = self._validate_columns(df, source_file)

        ingestion_timestamp = datetime.utcnow()

        df = (
            df.withColumn("ingestion_timestamp", lit(ingestion_timestamp.isoformat()))
            .withColumn("source_file", lit(source_file))
            .withColumn(
                "raw_record_hash",
                spark_hash(
                    *[
                        col(c)
                        for c in df.columns
                        if c
                        not in ["ingestion_timestamp", "source_file", "raw_record_hash"]
                    ]
                ),
            )
        )

        record_count = df.count()
        self.logger.info(f"Ingested {record_count} records from {csv_path}")

        return df

    def write_bronze(self, df: DataFrame, output_path: str):
        if not output_path or not output_path.strip():
            raise ValueError(f"Output path is empty: {output_path}")
        
        # Validate DataFrame is not empty and has required column
        if df is None:
            raise ValueError("DataFrame is None")
        
        if "assessment_year" not in df.columns:
            raise ValueError("assessment_year column is missing from DataFrame")
        
        if output_path.startswith("s3://"):
            output_path = output_path.rstrip("/")
            parts = output_path.replace("s3://", "").split("/", 1)
            if len(parts) == 1 or (len(parts) == 2 and not parts[1].strip()):
                bucket_name = parts[0]
                output_path = f"s3://{bucket_name}/bronze"
                self.logger.info(f"Path was bucket root only, added default path component: {output_path}")
        
        if not output_path or not output_path.strip():
            raise ValueError(f"Output path is empty after processing: {output_path}")
        
        self.logger.info(f"Final output path: {output_path}")
        
        record_count = df.count()
        self.logger.info(f"Writing {record_count} records to Bronze layer: {output_path}")
        
        if record_count == 0:
            self.logger.warning("No records to write to Bronze layer")
            return
        
        distinct_years = df.select("assessment_year").distinct().collect()
        partition_values = [row.assessment_year for row in distinct_years]
        self.logger.info(f"Partition values (assessment_year): {partition_values}")
        
        for year in partition_values:
            if year is None:
                raise ValueError("Found None value in assessment_year partition column")
            if not isinstance(year, int):
                raise ValueError(f"Invalid assessment_year value type: {type(year)}, value: {year}")
        
        try:
            df.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .partitionBy("assessment_year") \
                .parquet(output_path)
            
            self.logger.info(f"Successfully wrote Bronze layer to {output_path}")
        except Exception as e:
            self.logger.error(f"Error writing Bronze layer to path '{output_path}': {str(e)}")
            self.logger.error(f"DataFrame schema: {df.schema}")
            self.logger.error(f"DataFrame columns: {df.columns}")
            self.logger.error(f"Partition column type: {dict(df.dtypes).get('assessment_year', 'NOT FOUND')}")
            self.logger.error(f"Record count: {record_count}")
            self.logger.error(f"Partition values: {partition_values}")
            raise
