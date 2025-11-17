# Tax Data Pipeline - Architecture & Design

# Overview

Production-ready tax data analytics platform using **Medallion Architecture** (Bronze → Silver → Gold) with **PySpark** and **Star Schema** dimensional modeling.

## Medallion Architecture Layers

```
BRONZE (Raw) → SILVER (Cleansed) → GOLD (Curated)
```

- **Bronze**: Raw CSV ingestion with metadata, schema validation, all records preserved
- **Silver**: Validation, cleaning, quality scoring, classification (Accept/Quarantine/Reject)
- **Gold**: Star schema (fact + dimensions), only Accept + Quarantine records

## Pipeline Components

```
pipeline/
├── config/
│   ├── pipeline_config.yaml      # Pipeline configuration
│   └── validation_rules.yaml     # Validation rules
├── src/
│   ├── bronze/csv_ingester.py     # CSV ingestion
│   ├── silver/                    # Validation, cleaning, quality scoring
│   ├── gold/dimensional_modeler.py # Star schema builder
│   ├── orchestration/pipeline_orchestrator.py
│   └── utils/                     # Logger, Spark session
└── tests/unit/                    # Unit tests
```

## Data Validation

### Validation Rules

| Rule | Logic | Handling |
|------|-------|----------|
| **NRIC** | Regex: `^[STFG]\d{7}[A-Z]$`, empty NOT allowed | Flag invalid, preserve original |
| **Postal Code** | 6-digit, range 10000-829999 | Standardize, flag invalid |
| **Filing Date** | After assessment_year, ISO or DD/MM/YYYY | Parse multiple formats, flag invalid |
| **Tax Calculation** | `chargeable_income = annual_income - total_reliefs`, tolerance 0.1 SGD | Flag discrepancies |
| **CPF** | CPF > 0 only for Residents | Auto-correct, flag violations |
| **Completeness** | All columns checked, any missing = failed | Flag missing values |

### Data Quality Scoring

**Weighted Formula:**
```
DQ Score = (NRIC × 0.25) + (PostalCode × 0.15) + (FilingDate × 0.20) + 
           (TaxCalculation × 0.15) + (CPF × 0.10) + (Completeness × 0.15)
```

**Classification:**
- **Accept** (≥ 0.8): Included in Gold, processed normally
- **Quarantine** (0.6-0.8): Included in Gold with `is_quarantined=true` flag
- **Reject** (< 0.6): Excluded from Gold, stored in rejected area

## Star Schema

### Dimension Tables

**`dim_taxpayer`**: Taxpayer demographics
- `taxpayer_key`, `taxpayer_id`, `nric`, `full_name`, `filing_status`, `residential_status`, `number_of_dependents`

**`dim_time`**: Time hierarchy
- `time_key`, `assessment_year`, `filing_year`, `filing_month`, `filing_quarter`, `filing_date`, `tax_season_flag`

**`dim_location`**: Singapore geography
- `location_key`, `postal_code`, `postal_code_raw`, `housing_type`, `region`, `planning_area`
- Region/planning_area derived from `postal_mapping.json` (extract first 2 digits, match against postal_codes array)

**`dim_occupation`**: Professional categorization
- `occupation_key`, `occupation`, `occupation_category`, `occupation_raw`
- Categories: Engineering, Management, Healthcare, Finance, Sales, Operations, Consulting, Legal, Education, Research, Other
- Keyword-based mapping from `pipeline_config.yaml`

### Fact Table

**`fact_tax_returns`**: Transaction-level tax data
- Foreign keys: `taxpayer_key`, `time_key`, `location_key`, `occupation_key`
- Measures: `annual_income_sgd`, `chargeable_income_sgd`, `tax_payable_sgd`, `tax_paid_sgd`, `total_reliefs_sgd`, `cpf_contributions_sgd`, `foreign_income_sgd`
- Calculated: `tax_compliance_rate`, `tax_liability`, `effective_tax_rate`
- Quality metrics: `data_quality_score`, `dq_classification`, quality flags
- Audit: `ingestion_timestamp`, `source_file`

## Technology Stack

- **Processing**: PySpark 3.x
- **Storage**: Parquet (partitioned by `assessment_year`)
- **Configuration**: YAML
- **Testing**: pytest
- **Containerization**: Docker

## Pipeline Flow

1. **Bronze**: CSV → Add metadata → Write Parquet (all records)
2. **Silver**: Validate → Clean → Score → Classify → Write Accept/Quarantine to Silver, Reject to Rejected
3. **Gold**: Build dimensions → Build fact table → Write star schema (Accept + Quarantine only)

## Key Design Decisions

1. **Parquet over CSV**: Better compression, schema support, query performance
2. **Soft Validation**: Flag for review, preserve audit trail
3. **SCD Type 1**: Overwrite dimensions (simpler for tax data)
4. **Partitioning**: By `assessment_year` (Bronze, Silver, Gold fact); dimensions not partitioned
5. **Local-First**: Test locally, deploy to cloud

## Assumptions

This architecture is built on the following assumptions that inform design decisions and implementation:

### Data Volume & Scale
- **Production Volume**: Pipeline designed to handle up to **100K records per batch** (as specified in requirements)
- **Processing Frequency**: Daily batch processing (2 AM UTC schedule)
- **Growth**: Architecture scales horizontally; assumes linear growth in data volume over time
- **Retention**: Historical data retained indefinitely in Gold layer; rejected data deleted after 1 year

### Singapore-Specific Assumptions
- **NRIC Format**: Assumes standard Singapore NRIC format `[STFG]\d{7}[A-Z]` (S=Singapore citizen, T=PR, F=Foreigner, G=Government-issued)
  - Note: Full NRIC checksum validation algorithm not implemented (as per requirements)
- **Postal Codes**: Assumes 6-digit Singapore postal codes in range 10000-829999
  - Region/planning area derived from first 2 digits using [`postal_mapping.json`](/pipeline/src/utils/postal_mapping.json)
- **Tax Year**: Assessment year aligns with calendar year (2023, 2024, etc.)

### Data Quality Thresholds
- **Accept Threshold**: Records with DQ score ≥ 0.8 are fully accepted into Gold layer
- **Quarantine Threshold**: Records with DQ score 0.6-0.8 are included in Gold with `is_quarantined=true` flag
- **Reject Threshold**: Records with DQ score < 0.6 are excluded from Gold, stored in rejected area
- **Quality Weights**: Assumes NRIC validation (25%) and Filing Date (20%) are most critical; weights are configurable
- **Tax Calculation Tolerance**: Allows 0.1 SGD difference between calculated and provided `chargeable_income` (accounts for rounding)

### Business Logic Assumptions
- **CPF Contributions**: Only Singapore residents can have CPF contributions > 0; non-residents auto-corrected to 0
- **Filing Date**: Must be after assessment year (logical constraint)
- **Tax Compliance**: `tax_compliance_rate = tax_paid / tax_payable`; ≥ 0.95 considered fully compliant
- **Tax Liability**: `tax_liability = tax_payable - tax_paid` (outstanding amount)
- **Effective Tax Rate**: `effective_tax_rate = tax_payable / annual_income` (pre-calculated for analytics)

### Schema & Data Modeling Assumptions
- **SCD Type 1**: Dimensions use Type 1 (overwrite) strategy; assumes no historical tracking needed for taxpayer demographics
- **Partitioning Strategy**: Fact tables partitioned by `assessment_year`; dimensions not partitioned (assumes small size)
- **Surrogate Keys**: All dimension tables use auto-generated surrogate keys; natural keys preserved for audit
- **Star Schema**: Assumes analytical queries benefit from denormalized star schema over normalized 3NF
- **Quarantine Records**: Quarantined records included in Gold for completeness; analysts can filter using `is_quarantined` flag

### Data Format Assumptions
- **Input Format**: CSV files with headers; assumes UTF-8 encoding
- **Date Formats**: Supports both ISO (YYYY-MM-DD) and DD/MM/YYYY formats; filing date parsed flexibly
- **Output Format**: Parquet with Snappy compression; assumes columnar storage benefits for analytics
- **Schema Evolution**: Assumes schema changes are infrequent; versioning handled through Data Catalog

### Occupation Categorization Assumptions
- **Keyword-Based Mapping**: Occupation categories assigned using keyword matching (case-insensitive)
- **Default Category**: Occupations not matching any keywords default to "Other" category
- **Categories**: 11 predefined categories (Engineering, Management, Healthcare, Finance, Sales, Operations, Consulting, Legal, Education, Research, Other)
- **Sample Size Filter**: Detailed occupation analysis filters to occupations with ≥ 5 returns (meaningful sample size)

### Geographic Mapping Assumptions
- **Postal Code Mapping**: First 2 digits of postal code used to lookup region/planning area in `postal_mapping.json`
- **Housing Type**: Assumed to be provided in source data; no derivation logic
- **Region Hierarchy**: Assumes region → planning area hierarchy for geographic analysis

### Performance & Processing Assumptions
- **Glue DPU Scaling**: Assumes 2-10 DPU range sufficient for 100K records; auto-scaling handles spikes
- **Job Duration**: Assumes typical job runs 5-15 minutes; cost estimates based on 10-minute average
- **Athena Query Performance**: Assumes partition pruning by `assessment_year` improves query performance
- **Concurrent Processing**: Assumes single job execution at a time; no concurrent batch conflicts

### Cost Assumptions
- **Monthly Cost Estimate**: Based on 100K records, daily processing, 10-minute job duration, 2 DPU
- **Storage Growth**: Assumes 10 GB initial storage; lifecycle policies archive to Glacier after 30 days
- **Query Volume**: Assumes 100 Athena queries per month at ~1 GB scanned each
- **CloudWatch Logs**: Assumes 5 GB log retention per month

### Security Assumptions
- **Encryption**: SSE-S3 encryption sufficient for data at rest; TLS 1.2+ for in-transit
- **Access Control**: IAM roles with least privilege; assumes no cross-account access needed
- **Network**: VPC with private subnets optional for production; assumes public S3 access acceptable for dev
- **Data Classification**: All resources tagged with PII and TaxData classifications

### Operational Assumptions
- **Error Handling**: Assumes failed jobs can be manually retried; no automatic retry logic
- **Monitoring**: CloudWatch logs sufficient for troubleshooting; assumes manual log review
- **Data Catalog**: Assumes manual table creation or crawler-based discovery; no automated schema evolution
- **Deployment**: Assumes manual deployment via AWS Console; Terraform optional (AWS SSO limitations noted)

### Data Lineage & Audit Assumptions
- **Source Tracking**: `source_file` and `ingestion_timestamp` preserved in fact table for audit
- **Quality Flags**: Individual validation flags preserved for data quality analysis
- **Rejected Records**: Rejected records stored separately for review; not included in analytics

## Configuration

- **`validation_rules.yaml`**: Validation patterns, rules, thresholds
- **`pipeline_config.yaml`**: Required columns, storage paths, occupation categories
- **`postal_mapping.json`**: Postal code to region/planning_area mapping

---
# AWS Cloud Architecture for Tax Data Pipeline

Serverless AWS architecture for deploying the tax data analytics pipeline.

**Related Documentation:**
- **Pipeline Architecture**: See `ARCHITECTURE_DESIGN.md`
- **Business Objectives**: See `BUSINESS_OBJECTIVES_MAPPING.md`

## Architecture

```
S3 Raw → Glue Scheduler → AWS Glue ETL → S3 (Bronze/Silver/Gold/Rejected)
                                       ↓
                                Glue Data Catalog
                                       ↓
                           Athena → QuickSight (Optional)
```

### Components

- **S3**: Data lake storage (raw, bronze, silver, gold, rejected, scripts, query-results)
- **Glue Scheduler**: Daily cron trigger (2 AM UTC)
- **AWS Glue**: PySpark ETL job (2-10 DPU, auto-scaling)
- **Glue Data Catalog**: Schema registry for Athena
- **Athena**: Serverless SQL queries on Parquet files
- **CloudWatch**: Monitoring and logging

## Service Selection Rationale

| Service | Purpose | Rationale |
|---------|---------|-----------|
| **S3** | Data Lake | Cost-effective, unlimited scale, Parquet support, versioning, encryption |
| **Glue** | ETL Processing | Serverless PySpark, auto-scaling, integrated Data Catalog, pay-per-use |
| **Athena** | SQL Analytics | Serverless, pay-per-query, direct S3 Parquet access |
| **IAM** | Access Control | Least privilege, role-based access |
| **CloudWatch** | Monitoring | Metrics, logs, alarms |


## Security

- **Encryption**: SSE-S3 at rest, TLS 1.2+ in transit
- **Access Control**: IAM roles with least privilege, bucket policies
- **Network**: VPC with private subnets (optional for production)
- **Audit**: CloudTrail for API logging, CloudWatch for pipeline logs
- **Data Classification**: Tags on all resources (PII, TaxData)

## S3 Bucket Structure

```
s3://tax-data-pipeline-raw-dev/              # Raw CSV files
s3://tax-data-pipeline-bronze-dev/           # Bronze layer
s3://tax-data-pipeline-silver-dev/           # Silver layer
s3://tax-data-pipeline-gold-dev/             # Gold layer (star schema)
s3://tax-data-pipeline-rejected-dev/         # Rejected records
s3://tax-data-pipeline-scripts-dev/          # Pipeline code
s3://tax-data-pipeline-query-results-dev/     # Athena results
```

**Features**: SSE-S3 encryption, versioning (data buckets), lifecycle policies (archive to Glacier after 30 days, delete rejected after 1 year)


## Glue Job Configuration

- **Name**: `tax-data-pipeline-etl-dev`
- **Type**: Spark
- **Glue Version**: 4.0
- **DPU**: 2-10 (auto-scaling)
- **Script**: `s3://tax-data-pipeline-scripts-dev/pipeline/main.py`
- **Dependencies**: `s3://tax-data-pipeline-scripts-dev/pipeline/dependencies.zip`
- **Schedule**: `cron(0 2 * * ? *)` (Daily at 2 AM UTC)
- **IAM Role**: `tax-data-pipeline-glue-service-role-dev`


## Deployment Steps

### 1. Create S3 Buckets

Create 7 buckets with:
- Block public access enabled
- Versioning enabled (data buckets)
- SSE-S3 encryption
- Lifecycle policies: Archive to Glacier after 30 days (raw/bronze/silver), delete after 1 year (rejected)
![](/Section1/docs/imgs/s3-buckets.png)


### 2. Create IAM Role

**Glue Service Role**: `tax-data-pipeline-glue-service-role-dev`
- Attach: `AWSGlueServiceRole` (managed policy)
- Create custom policies:
  - S3 access (read/write to all data buckets)
  - Glue Data Catalog access
  - CloudWatch Logs access
![](/Section1/docs/imgs/iam-glue-role.png)

### 3. Create Glue Data Catalog Database

- **Name**: `tax_data_pipeline_database_dev`
- Tables created automatically by crawlers or manually

### 4. Upload Pipeline Code

```bash
cd pipeline
zip -r dependencies.zip src/ config/
aws s3 cp main.py s3://tax-data-pipeline-scripts-dev/pipeline/
aws s3 cp dependencies.zip s3://tax-data-pipeline-scripts-dev/pipeline/
```

Update `pipeline_config.yaml` with S3 bucket paths before creating zip.

### 5. Create Glue ETL Job

- **Job name**: `tax-data-pipeline-etl-dev`
- **IAM Role**: `tax-data-pipeline-glue-service-role-dev`
- **Type**: Spark, Glue 4.0
- **Script**: `s3://tax-data-pipeline-scripts-dev/pipeline/main.py`
- **Python library path**: `s3://tax-data-pipeline-scripts-dev/pipeline/dependencies.zip`
- **Job parameters**: `--csv-path s3://tax-data-pipeline-raw-dev/individual_tax_returns.csv`
- **Workers**: 2, Type: G.1X

### 6. Schedule Glue Job

- **Schedule name**: `tax-data-pipeline-daily-schedule-dev`
- **Cron**: `0 2 * * ? *` (Daily at 2 AM UTC)
- **State**: Activated
![](/Section1/docs/imgs/glue-job.png)


### 7. Create Athena Workgroup

- **Name**: `tax-data-pipeline-analytics-workgroup-dev`
- **Query result location**: `s3://tax-data-pipeline-query-results-dev/`
- **Encryption**: SSE-S3

### 8. Create Glue Crawlers (Optional)

Create crawlers for Bronze, Silver, and Gold layers to auto-discover schemas and create Data Catalog tables.

### 9. Test Pipeline

1. Upload test CSV to `s3://tax-data-pipeline-raw-dev/`
2. Run Glue job manually
3. Verify output in Bronze, Silver, Gold, Rejected buckets
4. Query data using Athena
![](/Section1/docs/imgs/cloudwatch.png)
![](/Section1/docs/imgs/glue-db.png)


## Cost Analysis

**Estimated Monthly Cost** (100K records, daily processing): **~$21-37**

| Service | Cost |
|---------|------|
| S3 Storage (10 GB) | $0.23 |
| S3 Requests | $0.05 |
| Glue Job (30 runs × 10 min × 2 DPU) | $18.00 |
| Athena (100 queries × 1 GB) | $0.50 |
| CloudWatch Logs (5 GB) | $2.50 |
| **Total** | **~$21.28** |

**Optimization**: Use lifecycle policies to archive to Glacier, optimize Athena queries with partition filters.


## Troubleshooting

1. **Glue job fails**: Check CloudWatch logs at `/aws-glue/jobs/output`
2. **Access denied**: Verify IAM role has S3 and Glue permissions
3. **Config files not found**: Ensure `dependencies.zip` includes `config/` and `src/utils/postal_mapping.json`
4. **Athena queries fail**: Verify Data Catalog tables exist and schemas match


## Infrastructure as Code

Sample Terraform scripts are available in `infrastructure/terraform/` for reference. Due to AWS SSO limitations, I am unable to use this option.
