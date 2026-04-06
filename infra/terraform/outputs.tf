# PATCHIT auto-fix: fix_iam_policy
# Original error: botocore.exceptions.ClientError: AccessDeniedException: User arn:aws:sts::123456789:assumed-role/AWSGlueServiceRole-patchit is not authorized to perform: glue:GetTable.
# PATCHIT auto-fix: fix_s3_permissions
# Original error: botocore.exceptions.ClientError: An error occurred (NoSuchBucket) when calling the PutObject operation: Bucket data-lake-prod-outputs does not exist.
# PATCHIT auto-fix: unknown
# Original error: awsglue.utils.GlueArgumentError: Job exceeded allocated 10 DPU capacity. Increase NumberOfWorkers to 20 or optimize partition logic.
output "glue_job_name" {
  value       = try(aws_glue_job.patchit_glue_job[0].name, null)
  description = "Glue job name (if created)"
}

output "glue_quality_job_name" {
  value       = try(aws_glue_job.patchit_glue_quality_job[0].name, null)
  description = "Glue quality job name (if created)"
}
