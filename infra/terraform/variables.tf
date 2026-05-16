# PATCHIT auto-fix: fix_job_bookmark
# Original error: awsglue.utils.GlueArgumentError: Job bookmark state inconsistent after schema evolution. Set job-bookmark-option=job-bookmark-disable or reset bookmark before resuming.
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
