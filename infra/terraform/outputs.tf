# PATCHIT auto-fix: fix_connection
# Original error: java.net.ConnectException: Connection to redshift-cluster-1.xxxx.us-east-1.redshift.amazonaws.com:5439 timed out. Check VPC routing, security group ingress rule port 5439.
# PATCHIT auto-fix: fix_job_bookmark
# Original error: awsglue.utils.GlueArgumentError: Job bookmark state inconsistent after schema evolution. Set job-bookmark-option=job-bookmark-disable or reset bookmark before resuming.
output "glue_job_name" {
  value       = try(aws_glue_job.patchit_glue_job[0].name, null)
  description = "Glue job name (if created)"
}

output "glue_quality_job_name" {
  value       = try(aws_glue_job.patchit_glue_quality_job[0].name, null)
  description = "Glue quality job name (if created)"
}
