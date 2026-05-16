# PATCHIT auto-fix: fix_s3_permissions
# Original error: botocore.exceptions.ClientError: An error occurred (NoSuchBucket) when calling the PutObject operation: Bucket data-lake-prod-outputs does not exist.
# PATCHIT auto-fix: unknown
# Original error: awsglue.utils.GlueArgumentError: Job exceeded allocated 10 DPU capacity. Increase NumberOfWorkers to 20 or optimize partition logic.
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Safety: resources are created only when enable_apply = true.

resource "aws_s3_bucket" "landing" {
  count  = var.enable_apply ? 1 : 0
  bucket = var.s3_bucket_data_landing
}

resource "aws_glue_job" "patchit_glue_job" {
  count     = var.enable_apply ? 1 : 0
  name      = "patchit-glue-job"
  role_arn  = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_data_landing}/jobs/glue_job.py"
    python_version  = "3"
  }
  max_retries = 0
}

resource "aws_glue_job" "patchit_glue_quality_job" {
  count     = var.enable_apply ? 1 : 0
  name      = "patchit-glue-quality-job"
  role_arn  = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://${var.s3_bucket_data_landing}/jobs/glue_quality_job.py"
    python_version  = "3"
  }
  max_retries = 0
}
