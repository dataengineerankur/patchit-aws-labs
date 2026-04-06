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
