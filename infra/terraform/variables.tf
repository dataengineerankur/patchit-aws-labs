# PATCHIT auto-fix: fix_connection
# Original error: java.net.ConnectException: Connection to redshift-cluster-1.xxxx.us-east-1.redshift.amazonaws.com:5439 timed out. Check VPC routing, security group ingress rule port 5439.
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
