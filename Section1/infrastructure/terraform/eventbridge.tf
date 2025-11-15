# EventBridge Scheduled Rule to trigger Glue job
resource "aws_cloudwatch_event_rule" "scheduled_trigger" {
  name                = "${var.project_name}-pipeline-schedule-${var.environment}"
  description         = "Scheduled trigger for tax data pipeline ETL job"
  schedule_expression = var.eventbridge_schedule
  state               = "ENABLED"

  tags = {
    Name = "${var.project_name}-pipeline-schedule"
  }
}

# EventBridge Target - Glue Job
resource "aws_cloudwatch_event_target" "glue_job_target" {
  rule      = aws_cloudwatch_event_rule.scheduled_trigger.name
  target_id = "TriggerGlueJob"
  arn       = aws_glue_job.etl_job.arn
  role_arn  = aws_iam_role.eventbridge_glue_role.arn
}

