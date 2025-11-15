output "s3_bucket_raw" {
  description = "S3 bucket for raw data"
  value       = aws_s3_bucket.raw.id
}

output "s3_bucket_bronze" {
  description = "S3 bucket for Bronze layer"
  value       = aws_s3_bucket.bronze.id
}

output "s3_bucket_silver" {
  description = "S3 bucket for Silver layer"
  value       = aws_s3_bucket.silver.id
}

output "s3_bucket_gold" {
  description = "S3 bucket for Gold layer"
  value       = aws_s3_bucket.gold.id
}

output "s3_bucket_rejected" {
  description = "S3 bucket for rejected records"
  value       = aws_s3_bucket.rejected.id
}

output "s3_bucket_scripts" {
  description = "S3 bucket for pipeline scripts"
  value       = aws_s3_bucket.scripts.id
}

output "s3_bucket_query_results" {
  description = "S3 bucket for Athena query results"
  value       = aws_s3_bucket.query_results.id
}

output "glue_job_name" {
  description = "Glue ETL job name"
  value       = aws_glue_job.etl_job.name
}

output "glue_job_arn" {
  description = "Glue ETL job ARN"
  value       = aws_glue_job.etl_job.arn
}

output "glue_service_role_arn" {
  description = "IAM role ARN for Glue service"
  value       = aws_iam_role.glue_service_role.arn
}

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.tax_database.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.analytics.name
}

output "eventbridge_rule_arn" {
  description = "EventBridge scheduled rule ARN"
  value       = aws_cloudwatch_event_rule.scheduled_trigger.arn
}

