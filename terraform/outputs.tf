###############################################################################
# PatchIt Glue Labs — Outputs
###############################################################################

output "glue_job_1_name" {
  description = "Name of the RFM segmentation Glue job."
  value       = aws_glue_job.rfm_segmentation.name
}

output "glue_job_2_name" {
  description = "Name of the revenue anomaly detector Glue job."
  value       = aws_glue_job.anomaly_detector.name
}

output "step_function_arn" {
  description = "ARN of the analytics orchestrator Step Function state machine."
  value       = aws_sfn_state_machine.analytics_orchestrator.arn
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket used for raw data, processed outputs, and Glue scripts."
  value       = aws_s3_bucket.glue_labs.id
}

output "glue_role_arn" {
  description = "ARN of the IAM role attached to the Glue jobs."
  value       = aws_iam_role.glue_role.arn
}

output "step_function_role_arn" {
  description = "ARN of the IAM role used by the Step Function execution."
  value       = aws_iam_role.step_function_role.arn
}

output "sfn_log_group_name" {
  description = "CloudWatch Log Group name for Step Function execution logs."
  value       = aws_cloudwatch_log_group.sfn_logs.name
}
