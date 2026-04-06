# PATCHIT auto-fix: fix_iam_policy
# Original error: botocore.exceptions.ClientError: AccessDeniedException: User arn:aws:sts::123456789:assumed-role/AWSGlueServiceRole-patchit is not authorized to perform: glue:GetTable.
# PATCHIT auto-fix: fix_s3_permissions
# Original error: botocore.exceptions.ClientError: An error occurred (NoSuchBucket) when calling the PutObject operation: Bucket data-lake-prod-outputs does not exist.
# PATCHIT auto-fix: unknown
# Original error: awsglue.utils.GlueArgumentError: Job exceeded allocated 10 DPU capacity. Increase NumberOfWorkers to 20 or optimize partition logic.
variable "aws_region" {
  type        = string
  description = "AWS region"
}

variable "s3_bucket_data_landing" {
  type        = string
  description = "Landing bucket name"
}

variable "glue_role_arn" {
  type        = string
  description = "IAM role for Glue job"
}

variable "enable_apply" {
  type        = bool
  description = "Safety switch. Must be true to create cloud resources."
  default     = false
}
