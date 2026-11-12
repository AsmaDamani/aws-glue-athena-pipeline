output "raw_bucket" {
  value = aws_s3_bucket.raw.id
}

output "curated_bucket" {
  value = aws_s3_bucket.curated.id
}

output "glue_database" {
  value = aws_glue_catalog_database.this.name
}

output "glue_job_name" {
  value = aws_glue_job.clean_events.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}
