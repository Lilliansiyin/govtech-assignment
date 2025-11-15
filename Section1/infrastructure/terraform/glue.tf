# Glue Data Catalog Database
resource "aws_glue_catalog_database" "tax_database" {
  name        = "${var.project_name}_database_${var.environment}"
  description = "Tax data analytics database"

  catalog_id = data.aws_caller_identity.current.account_id
}

# Glue ETL Job
resource "aws_glue_job" "etl_job" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_service_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${coalesce(var.s3_script_bucket, aws_s3_bucket.scripts.id)}/${var.s3_script_key}"
    python_version  = "3"
  }

  default_arguments = {
    "--extra-py-files"           = "s3://${coalesce(var.s3_script_bucket, aws_s3_bucket.scripts.id)}/${var.s3_dependencies_key}"
    "--enable-metrics"            = ""
    "--enable-spark-ui"           = "true"
    "--spark-event-logs-path"    = "s3://${aws_s3_bucket.query_results.id}/spark-logs/"
    "--TempDir"                   = "s3://${aws_s3_bucket.query_results.id}/temp/"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  glue_version = var.glue_version

  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  max_retries = 1
  timeout     = var.glue_timeout

  execution_property {
    max_concurrent_runs = 1
  }

  # VPC Configuration (optional, for production)
  # Note: Requires aws_glue_connection resource when enable_vpc is true
  connections = var.enable_vpc ? [aws_glue_connection.vpc_connection[0].name] : []

  tags = {
    Name = var.glue_job_name
  }
}

# Glue Connection for VPC (required when enable_vpc is true)
resource "aws_glue_connection" "vpc_connection" {
  count = var.enable_vpc ? 1 : 0

  name = "${var.project_name}-vpc-connection-${var.environment}"

  connection_type = "NETWORK"

  connection_properties = {
    JDBC_CONNECTION_URL = ""
  }

  physical_connection_requirements {
    security_group_id_list = var.security_group_ids
    subnet_id              = length(var.subnet_ids) > 0 ? var.subnet_ids[0] : ""
  }

  tags = {
    Name = "${var.project_name}-vpc-connection"
  }
}

