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
