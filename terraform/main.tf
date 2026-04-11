###############################################################################
# PatchIt Glue Labs — Terraform Infrastructure
# AWS Account : 244206439389
# Region      : us-east-1
###############################################################################

terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.40"
    }
  }
}

provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key

  default_tags {
    tags = {
      Project     = "PatchItGlueLabs"
      Environment = "lab"
      ManagedBy   = "Terraform"
    }
  }
}

###############################################################################
# S3 Bucket
###############################################################################

resource "aws_s3_bucket" "glue_labs" {
  bucket        = "patchit-glue-labs-244206439389"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "glue_labs" {
  bucket = aws_s3_bucket.glue_labs.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_labs" {
  bucket = aws_s3_bucket.glue_labs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "glue_labs" {
  bucket                  = aws_s3_bucket.glue_labs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "glue_labs" {
  bucket = aws_s3_bucket.glue_labs.id

  rule {
    id     = "expire-temp-outputs"
    status = "Enabled"

    filter {
      prefix = "processed/"
    }

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

###############################################################################
# S3 Objects — Glue Script uploads
###############################################################################

resource "aws_s3_object" "rfm_segmentation_script" {
  bucket = aws_s3_bucket.glue_labs.id
  key    = "scripts/customer_rfm_segmentation.py"
  source = "${path.module}/../glue_scripts/customer_rfm_segmentation.py"
  etag   = filemd5("${path.module}/../glue_scripts/customer_rfm_segmentation.py")

  depends_on = [aws_s3_bucket.glue_labs]
}

resource "aws_s3_object" "anomaly_detector_script" {
  bucket = aws_s3_bucket.glue_labs.id
  key    = "scripts/revenue_anomaly_detector.py"
  source = "${path.module}/../glue_scripts/revenue_anomaly_detector.py"
  etag   = filemd5("${path.module}/../glue_scripts/revenue_anomaly_detector.py")

  depends_on = [aws_s3_bucket.glue_labs]
}

resource "aws_s3_object" "sample_customers" {
  bucket = aws_s3_bucket.glue_labs.id
  key    = "raw/customers/customers.csv"
  source = "${path.module}/../data/customers.csv"
  etag   = filemd5("${path.module}/../data/customers.csv")

  depends_on = [aws_s3_bucket.glue_labs]
}

resource "aws_s3_object" "sample_orders" {
  bucket = aws_s3_bucket.glue_labs.id
  key    = "raw/orders/orders.csv"
  source = "${path.module}/../data/orders.csv"
  etag   = filemd5("${path.module}/../data/orders.csv")

  depends_on = [aws_s3_bucket.glue_labs]
}

resource "aws_s3_object" "sample_revenue_daily" {
  bucket = aws_s3_bucket.glue_labs.id
  key    = "raw/revenue_daily/revenue_daily.csv"
  source = "${path.module}/../data/revenue_daily.csv"
  etag   = filemd5("${path.module}/../data/revenue_daily.csv")

  depends_on = [aws_s3_bucket.glue_labs]
}

###############################################################################
# IAM — Glue Execution Role
###############################################################################

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    sid     = "GlueAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "PatchItGlueRole"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  description        = "Execution role for PatchIt Glue ETL jobs"
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glue_s3_full_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy" "glue_cloudwatch_logs" {
  name = "PatchItGlueCloudWatchLogs"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:AssociateKmsKey"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:244206439389:log-group:/aws-glue/*"
      }
    ]
  })
}

###############################################################################
# AWS Glue Jobs
###############################################################################

resource "aws_glue_job" "rfm_segmentation" {
  name              = "patchit-customer-rfm-segmentation"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  max_retries       = 1
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_labs.id}/scripts/customer_rfm_segmentation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                       = "python"
    "--job-bookmark-option"                = "job-bookmark-disable"
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"   = "true"
    "--enable-spark-ui"                    = "true"
    "--spark-event-logs-path"              = "s3://${aws_s3_bucket.glue_labs.id}/spark-logs/rfm-segmentation/"
    "--TempDir"                            = "s3://${aws_s3_bucket.glue_labs.id}/tmp/"
    "--extra-py-files"                     = ""
    "--customers_path"                     = "s3://${aws_s3_bucket.glue_labs.id}/raw/customers/customers.csv"
    "--orders_path"                        = "s3://${aws_s3_bucket.glue_labs.id}/raw/orders/orders.csv"
    "--output_path"                        = "s3://${aws_s3_bucket.glue_labs.id}/processed/rfm_segments/"
    "--recency_weight"                     = "0.3"
    "--frequency_weight"                   = "0.35"
    "--monetary_weight"                    = "0.35"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_s3_object.rfm_segmentation_script,
    aws_iam_role_policy_attachment.glue_service_policy,
  ]
}

resource "aws_glue_job" "anomaly_detector" {
  name              = "patchit-revenue-anomaly-detector"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 5
  max_retries       = 1
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_labs.id}/scripts/revenue_anomaly_detector.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                       = "python"
    "--job-bookmark-option"                = "job-bookmark-disable"
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"   = "true"
    "--enable-spark-ui"                    = "true"
    "--spark-event-logs-path"              = "s3://${aws_s3_bucket.glue_labs.id}/spark-logs/anomaly-detector/"
    "--TempDir"                            = "s3://${aws_s3_bucket.glue_labs.id}/tmp/"
    "--rfm_segments_path"                  = "s3://${aws_s3_bucket.glue_labs.id}/processed/rfm_segments/"
    "--revenue_daily_path"                 = "s3://${aws_s3_bucket.glue_labs.id}/raw/revenue_daily/revenue_daily.csv"
    "--alerts_output_path"                 = "s3://${aws_s3_bucket.glue_labs.id}/processed/revenue_alerts/"
    "--zscore_threshold"                   = "2.5"
    "--bollinger_std_multiplier"           = "2.0"
    "--lookback_days"                      = "30"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  depends_on = [
    aws_s3_object.anomaly_detector_script,
    aws_iam_role_policy_attachment.glue_service_policy,
  ]
}

###############################################################################
# IAM — Step Functions Execution Role
###############################################################################

data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    sid     = "StepFunctionsAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = ["244206439389"]
    }
  }
}

resource "aws_iam_role" "step_function_role" {
  name               = "PatchItStepFunctionRole"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
  description        = "Execution role for PatchIt analytics Step Function"
}

resource "aws_iam_role_policy" "step_function_inline" {
  name = "PatchItStepFunctionInlinePolicy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueJobControl"
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns",
          "glue:BatchStopJobRun",
          "glue:GetJob"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:244206439389:job/patchit-customer-rfm-segmentation",
          "arn:aws:glue:${var.aws_region}:244206439389:job/patchit-revenue-anomaly-detector"
        ]
      },
      {
        Sid    = "CloudWatchLogsForSFN"
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      },
      {
        Sid    = "XRayAccess"
        Effect = "Allow"
        Action = [
          "xray:PutTraceSegments",
          "xray:PutTelemetryRecords",
          "xray:GetSamplingRules",
          "xray:GetSamplingTargets"
        ]
        Resource = "*"
      }
    ]
  })
}

###############################################################################
# CloudWatch Log Group for Step Functions
###############################################################################

resource "aws_cloudwatch_log_group" "sfn_logs" {
  name              = "/aws/states/patchit-analytics-orchestrator"
  retention_in_days = 30
}

###############################################################################
# Step Functions State Machine
###############################################################################

resource "aws_sfn_state_machine" "analytics_orchestrator" {
  name     = "patchit-analytics-orchestrator"
  role_arn = aws_iam_role.step_function_role.arn

  definition = file("${path.module}/../step_function/analytics_orchestrator.json")

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_logs.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }

  depends_on = [
    aws_glue_job.rfm_segmentation,
    aws_glue_job.anomaly_detector,
    aws_iam_role_policy.step_function_inline,
    aws_cloudwatch_log_group.sfn_logs,
  ]
}
