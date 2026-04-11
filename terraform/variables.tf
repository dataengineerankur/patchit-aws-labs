###############################################################################
# PatchIt Glue Labs — Input Variables
###############################################################################

variable "aws_access_key" {
  description = "AWS access key ID. Prefer setting via AWS_ACCESS_KEY_ID environment variable."
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_secret_key" {
  description = "AWS secret access key. Prefer setting via AWS_SECRET_ACCESS_KEY environment variable."
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_region" {
  description = "AWS region to deploy all resources into."
  type        = string
  default     = "us-east-1"

  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-[0-9]$", var.aws_region))
    error_message = "aws_region must be a valid AWS region identifier, e.g. us-east-1."
  }
}

variable "glue_role_arn" {
  description = "ARN of the IAM role used by Glue jobs. Computed from the aws_iam_role resource if left empty."
  type        = string
  default     = "arn:aws:iam::244206439389:role/PatchItGlueRole"
}
