variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "ap-southeast-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "tax-data-pipeline"
}

variable "glue_job_name" {
  description = "Name of the Glue ETL job"
  type        = string
  default     = "tax-pipeline-etl"
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type"
  type        = string
  default     = "G.1X"
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 2
}

variable "glue_max_workers" {
  description = "Maximum number of Glue workers for auto-scaling"
  type        = number
  default     = 10
}

variable "glue_timeout" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 60
}

variable "eventbridge_schedule" {
  description = "EventBridge cron schedule expression"
  type        = string
  default     = "cron(0 2 * * ? *)" # Daily at 2 AM UTC
}

variable "enable_vpc" {
  description = "Enable VPC for Glue jobs (recommended for production)"
  type        = bool
  default     = false # Set to true for production
}

variable "vpc_id" {
  description = "VPC ID for Glue jobs (required if enable_vpc is true)"
  type        = string
  default     = ""
}

variable "subnet_ids" {
  description = "Subnet IDs for Glue jobs (required if enable_vpc is true)"
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "Security group IDs for Glue jobs (required if enable_vpc is true)"
  type        = list(string)
  default     = []
}

variable "s3_script_bucket" {
  description = "S3 bucket name for pipeline scripts"
  type        = string
  default     = ""
}

variable "s3_script_key" {
  description = "S3 key for main.py script"
  type        = string
  default     = "pipeline/main.py"
}

variable "s3_dependencies_key" {
  description = "S3 key for dependencies.zip"
  type        = string
  default     = "pipeline/dependencies.zip"
}

