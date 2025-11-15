import json
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    monotonically_increasing_id,
    month,
    quarter,
    regexp_extract,
    row_number,
    to_date,
    trim,
    upper,
    when,
    year,
)
from pyspark.sql.window import Window

from src.utils.logger import StructuredLogger


def _categorize_occupation_udf(occupation_categories: Dict) -> callable:
    def categorize(occupation: str) -> str:
        if not occupation:
            return "Other"

        occupation_upper = occupation.upper()

        for category, keywords in occupation_categories.items():
            if category == "Other":
                continue
            for keyword in keywords:
                if keyword.upper() in occupation_upper:
                    return category

        return "Other"

    return categorize


class DimensionalModeler:
    def __init__(self, spark: SparkSession, config: Dict, logger: StructuredLogger):
        self.spark = spark
        self.config = config
        self.logger = logger
        self.occupation_categories = config.get("occupation_categories", {})
        self._postal_mapping = None

    def _load_postal_mapping(self, postal_mapping_path: str) -> List[Dict]:
        if self._postal_mapping is None:
            with open(postal_mapping_path, "r") as f:
                self._postal_mapping = json.load(f)
        return self._postal_mapping

    def _get_region_from_postal(
        self, postal_code: str, postal_mapping_path: str
    ) -> tuple:
        if not postal_code or len(postal_code) < 2:
            return (None, None)

        prefix = postal_code[:2]
        mapping = self._load_postal_mapping(postal_mapping_path)

        for entry in mapping:
            if prefix in entry.get("postal_codes", []):
                return (entry.get("region"), entry.get("area"))

        return (None, None)

    def build_dim_taxpayer(self, df: DataFrame) -> DataFrame:
        self.logger.info("Building dim_taxpayer")

        dim_taxpayer = df.select(
            col("taxpayer_id").alias("taxpayer_id"),
            col("nric_cleansed").alias("nric"),
            col("full_name"),
            col("filing_status"),
            col("residential_status"),
            col("number_of_dependents"),
        ).distinct()

        dim_taxpayer = dim_taxpayer.withColumn(
            "taxpayer_key", row_number().over(Window.partitionBy(lit(1)).orderBy("taxpayer_id")) - 1
        )

        dim_taxpayer = dim_taxpayer.select(
            "taxpayer_key",
            "taxpayer_id",
            "nric",
            "full_name",
            "filing_status",
            "residential_status",
            "number_of_dependents",
        )

        self.logger.info(f"Created dim_taxpayer with {dim_taxpayer.count()} records")
        return dim_taxpayer

    def build_dim_time(self, df: DataFrame) -> DataFrame:
        self.logger.info("Building dim_time")

        df_with_dates = df.filter(col("filing_date_cleansed").isNotNull())

        dim_time = df_with_dates.select(
            col("assessment_year"),
            year(col("filing_date_cleansed")).alias("filing_year"),
            month(col("filing_date_cleansed")).alias("filing_month"),
            quarter(col("filing_date_cleansed")).alias("filing_quarter"),
            col("filing_date_cleansed").alias("filing_date"),
        ).distinct()

        dim_time = dim_time.withColumn("is_valid_date", col("filing_date").isNotNull())

        dim_time = dim_time.withColumn(
            "tax_season_flag",
            when(col("filing_month") <= 3, "Early")
            .when(col("filing_month") <= 4, "On-time")
            .otherwise("Late"),
        )

        dim_time = dim_time.withColumn(
            "time_key",
            row_number().over(Window.partitionBy(lit(1)).orderBy("assessment_year", "filing_date")) - 1,
        )

        dim_time = dim_time.select(
            "time_key",
            "assessment_year",
            "filing_year",
            "filing_month",
            "filing_quarter",
            "filing_date",
            "is_valid_date",
            "tax_season_flag",
        )

        self.logger.info(f"Created dim_time with {dim_time.count()} records")
        return dim_time

    def build_dim_location(self, df: DataFrame, postal_mapping_path: str) -> DataFrame:
        self.logger.info("Building dim_location")

        mapping = self._load_postal_mapping(postal_mapping_path)

        dim_location = df.select(
            col("postal_code_cleansed").alias("postal_code"),
            col("postal_code").alias("postal_code_raw"),
            col("housing_type"),
        ).distinct()

        postal_mapping_data = []
        for entry in mapping:
            region = entry.get("region")
            area = entry.get("area")
            for prefix in entry.get("postal_codes", []):
                postal_mapping_data.append((prefix, region, area))

        from pyspark.sql.types import StringType, StructField, StructType

        postal_mapping_schema = StructType(
            [
                StructField("postal_prefix", StringType(), False),
                StructField("region", StringType(), True),
                StructField("planning_area", StringType(), True),
            ]
        )

        postal_mapping_df = self.spark.createDataFrame(
            postal_mapping_data, postal_mapping_schema
        )

        dim_location = dim_location.withColumn(
            "postal_prefix",
            when(
                col("postal_code").isNotNull() & (col("postal_code").rlike("^\\d{2,}")),
                regexp_extract(col("postal_code"), r"^(\d{2})", 1),
            ).otherwise(lit(None)),
        )

        dim_location = (
            dim_location.join(
                postal_mapping_df,
                dim_location["postal_prefix"] == postal_mapping_df["postal_prefix"],
                "left",
            )
            .drop(postal_mapping_df["postal_prefix"])
            .drop("postal_prefix")
        )

        dim_location = dim_location.withColumn(
            "is_valid_postal_code",
            (
                col("postal_code").isNotNull()
                & col("postal_code").rlike("^\\d{6}$")
                & col("postal_code").cast("int").between(10000, 829999)
            ),
        )

        dim_location = dim_location.withColumn(
            "location_key", row_number().over(Window.partitionBy(lit(1)).orderBy("postal_code")) - 1
        )

        dim_location = dim_location.select(
            "location_key",
            "postal_code",
            "postal_code_raw",
            "housing_type",
            "region",
            "planning_area",
            "is_valid_postal_code",
        )

        self.logger.info(f"Created dim_location with {dim_location.count()} records")
        return dim_location

    def build_dim_occupation(self, df: DataFrame) -> DataFrame:
        self.logger.info("Building dim_occupation")

        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType

        categorize_func = _categorize_occupation_udf(self.occupation_categories)
        categorize_udf = udf(categorize_func, StringType())

        dim_occupation = df.select(col("occupation").alias("occupation_raw")).distinct()

        dim_occupation = dim_occupation.withColumn(
            "occupation", trim(upper(col("occupation_raw")))
        )

        dim_occupation = dim_occupation.withColumn(
            "occupation_category", categorize_udf(col("occupation_raw"))
        )

        dim_occupation = dim_occupation.withColumn(
            "occupation_key", row_number().over(Window.partitionBy(lit(1)).orderBy("occupation")) - 1
        )

        dim_occupation = dim_occupation.select(
            "occupation_key", "occupation", "occupation_category", "occupation_raw"
        )

        self.logger.info(
            f"Created dim_occupation with {dim_occupation.count()} records"
        )
        return dim_occupation

    def build_fact_tax_returns(
        self,
        df: DataFrame,
        dim_taxpayer: DataFrame,
        dim_time: DataFrame,
        dim_location: DataFrame,
        dim_occupation: DataFrame,
    ) -> DataFrame:
        self.logger.info("Building fact_tax_returns")

        df_alias = df.alias("df")
        dim_taxpayer_alias = dim_taxpayer.alias("dim_taxpayer")
        dim_time_alias = dim_time.alias("dim_time")
        dim_location_alias = dim_location.alias("dim_location")
        dim_occupation_alias = dim_occupation.alias("dim_occupation")

        df_with_keys = (
            df_alias.join(
                dim_taxpayer_alias,
                col("df.taxpayer_id") == col("dim_taxpayer.taxpayer_id"),
                "left",
            )
            .join(
                dim_time_alias,
                (col("df.assessment_year") == col("dim_time.assessment_year"))
                & (col("df.filing_date_cleansed") == col("dim_time.filing_date")),
                "left",
            )
            .join(
                dim_location_alias,
                col("df.postal_code_cleansed") == col("dim_location.postal_code"),
                "left",
            )
            .join(
                dim_occupation_alias,
                col("df.occupation") == col("dim_occupation.occupation_raw"),
                "left",
            )
        )

        fact = df_with_keys.select(
            monotonically_increasing_id().alias("fact_key"),
            col("dim_taxpayer.taxpayer_key"),
            col("dim_time.time_key"),
            col("dim_location.location_key"),
            col("dim_occupation.occupation_key"),
            col("df.assessment_year").alias("assessment_year"),
            col("df.annual_income_sgd"),
            col("df.chargeable_income_sgd"),
            col("df.tax_payable_sgd"),
            col("df.tax_paid_sgd"),
            col("df.total_reliefs_sgd"),
            col("df.cpf_contributions_sgd"),
            col("df.foreign_income_sgd"),
            (
                col("df.tax_paid_sgd")
                / when(col("df.tax_payable_sgd") == 0, lit(1)).otherwise(
                    col("df.tax_payable_sgd")
                )
            ).alias("tax_compliance_rate"),
            (col("df.tax_payable_sgd") - col("df.tax_paid_sgd")).alias("tax_liability"),
            (
                col("df.tax_payable_sgd")
                / when(col("df.annual_income_sgd") == 0, lit(1)).otherwise(
                    col("df.annual_income_sgd")
                )
            ).alias("effective_tax_rate"),
            col("df.data_quality_score"),
            col("df.dq_classification"),
            col("df.is_valid_record"),
            col("df.is_quarantined"),
            col("df.is_rejected"),
            col("df.has_invalid_nric"),
            col("df.has_invalid_postal_code"),
            col("df.has_invalid_date"),
            col("df.has_calculation_error"),
            col("df.has_business_rule_violation"),
            col("df.has_completeness_issue"),
            col("df.ingestion_timestamp"),
            col("df.source_file"),
        )

        self.logger.info(f"Created fact_tax_returns with {fact.count()} records")
        return fact

    def build_star_schema(
        self, df: DataFrame, postal_mapping_path: str
    ) -> Dict[str, DataFrame]:
        dim_taxpayer = self.build_dim_taxpayer(df)
        dim_time = self.build_dim_time(df)
        dim_location = self.build_dim_location(df, postal_mapping_path)
        dim_occupation = self.build_dim_occupation(df)

        fact_tax_returns = self.build_fact_tax_returns(
            df, dim_taxpayer, dim_time, dim_location, dim_occupation
        )

        return {
            "dim_taxpayer": dim_taxpayer,
            "dim_time": dim_time,
            "dim_location": dim_location,
            "dim_occupation": dim_occupation,
            "fact_tax_returns": fact_tax_returns,
        }

    def write_gold_layer(self, star_schema: Dict[str, DataFrame], output_path: str):
        self.logger.info(f"Writing Gold layer to {output_path}")
        
        # Normalize S3 path - ensure it has a path component (not just bucket root)
        # Spark requires a path within the bucket, not just the bucket root
        if output_path.startswith("s3://") or output_path.startswith("s3a://"):
            output_path = output_path.rstrip("/")
            parts = output_path.replace("s3://", "").replace("s3a://", "").split("/", 1)
            if len(parts) == 1 or (len(parts) == 2 and not parts[1].strip()):
                bucket_name = parts[0]
                protocol = "s3://" if output_path.startswith("s3://") else "s3a://"
                output_path = f"{protocol}{bucket_name}/gold"
                self.logger.info(f"Path was bucket root only, added default path component: {output_path}")

        for table_name, df in star_schema.items():
            record_count = df.count()
            if record_count == 0:
                self.logger.warning(f"No records to write for {table_name}, skipping")
                continue
            
            if table_name == "fact_tax_returns":
                initial_count = record_count
                null_count = df.filter(col("assessment_year").isNull()).count()
                if null_count > 0:
                    self.logger.warning(
                        f"Found {null_count} records with null assessment_year in {table_name} out of {initial_count} total. Filtering them out."
                    )
                    df = df.filter(col("assessment_year").isNotNull())
                    remaining_null = df.filter(col("assessment_year").isNull()).count()
                    if remaining_null > 0:
                        self.logger.error(f"Still found {remaining_null} null assessment_year values after filtering. This should not happen.")
                        raise ValueError(f"Cannot write partitioned data with null assessment_year values. Found {remaining_null} null values.")
                    record_count = df.count()
                
            table_path = f"{output_path}{table_name}/" if output_path.endswith("/") else f"{output_path}/{table_name}/"

            try:
                if table_name == "fact_tax_returns":
                    df.write \
                        .mode("overwrite") \
                        .option("compression", "snappy") \
                        .partitionBy("assessment_year") \
                        .parquet(table_path)
                else:
                    df.write \
                        .mode("overwrite") \
                        .option("compression", "snappy") \
                        .parquet(table_path)

                self.logger.info(f"Wrote {table_name} ({record_count} records) to {table_path}")
            except Exception as e:
                self.logger.error(f"Error writing {table_name} to {table_path}: {str(e)}")
                raise
