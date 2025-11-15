# Infrastructure Deployment

Infrastructure as Code (Terraform) scripts for the Tax Data Pipeline.

**For manual deployment instructions, see `docs/CLOUD_ARCHITECTURE.md`.**

## Quick Start

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your values
terraform init
terraform plan
terraform apply
```

## Infrastructure Components

### S3 Buckets (7 buckets)
- `tax-data-pipeline-raw-{env}` - Raw CSV files
- `tax-data-pipeline-bronze-{env}` - Bronze layer
- `tax-data-pipeline-silver-{env}` - Silver layer
- `tax-data-pipeline-gold-{env}` - Gold layer
- `tax-data-pipeline-rejected-{env}` - Rejected records
- `tax-data-pipeline-scripts-{env}` - Pipeline code
- `tax-data-pipeline-query-results-{env}` - Athena results

**Features**: SSE-S3 encryption, versioning, lifecycle policies (Glacier after 90 days)

### IAM Roles
- `tax-data-pipeline-glue-service-role-{env}` - Glue ETL execution with S3, Glue Catalog, CloudWatch permissions

### AWS Glue
- **ETL Job**: `tax-pipeline-etl-{env}` (PySpark, 2-10 DPU, auto-scaling)
- **Data Catalog Database**: `tax_data_pipeline_database_{env}`
- **Schedule**: Daily at 2 AM UTC via Glue Native Scheduler

### Athena
- **Workgroup**: `tax-data-pipeline-analytics-workgroup-{env}`

## Configuration

See `terraform/terraform.tfvars.example` for variables:
- `aws_region` (default: ap-southeast-1)
- `environment` (dev/staging/prod)
- `project_name` (default: tax-data-pipeline)
- `glue_number_of_workers` (default: 2)
- `glue_max_workers` (default: 10)

## Upload Pipeline Code

```bash
cd ../../pipeline
zip -r dependencies.zip src/ config/
SCRIPTS_BUCKET=$(cd ../infrastructure/terraform && terraform output -raw s3_bucket_scripts)
aws s3 cp main.py s3://${SCRIPTS_BUCKET}/pipeline/
aws s3 cp dependencies.zip s3://${SCRIPTS_BUCKET}/pipeline/
```

## Cost

Estimated monthly cost: **~$21-25** (100K records, daily processing)

## Troubleshooting

1. **Bucket name exists**: Change `project_name` in `terraform.tfvars`
2. **Glue job fails**: Check CloudWatch logs at `/aws-glue/jobs/output`
3. **IAM permissions**: Ensure credentials have S3, Glue, IAM, EventBridge, Athena permissions

