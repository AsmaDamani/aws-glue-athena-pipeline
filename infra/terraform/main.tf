terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

locals {
  prefix = "${var.project}-${var.env}"
}

# ---------- Storage ----------
resource "aws_s3_bucket" "raw" {
  bucket = "${local.prefix}-raw"
}

resource "aws_s3_bucket" "curated" {
  bucket = "${local.prefix}-curated"
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${local.prefix}-artifacts"
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${local.prefix}-athena-results"
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    raw       = aws_s3_bucket.raw.id
    curated   = aws_s3_bucket.curated.id
    artifacts = aws_s3_bucket.artifacts.id
    athena    = aws_s3_bucket.athena_results.id
  }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 30
    }
  }
}

# ---------- Code ----------
resource "aws_s3_object" "job_script" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/clean_events.py"
  source = "${path.module}/../../src/glue_jobs/clean_events.py"
  etag   = filemd5("${path.module}/../../src/glue_jobs/clean_events.py")
}

resource "aws_s3_object" "pipeline_package" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/pipeline.zip"
  source = "${path.module}/../../build/pipeline.zip"
  etag   = filemd5("${path.module}/../../build/pipeline.zip")
}

# ---------- Catalog ----------
resource "aws_glue_catalog_database" "this" {
  name = replace("${local.prefix}_db", "-", "_")
}

# ---------- IAM ----------
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.prefix}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

data "aws_iam_policy_document" "glue_s3" {
  statement {
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.raw.arn, "${aws_s3_bucket.raw.arn}/*",
      aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*",
    ]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.curated.arn, "${aws_s3_bucket.curated.arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

# ---------- Job + schedule ----------
resource "aws_glue_job" "clean_events" {
  name              = "${local.prefix}-clean-events"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = 60

  command {
    script_location = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.job_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--RAW_PATH"                         = "s3://${aws_s3_bucket.raw.id}/events/"
    "--CURATED_PATH"                     = "s3://${aws_s3_bucket.curated.id}/"
    "--DATABASE"                         = aws_glue_catalog_database.this.name
    "--extra-py-files"                   = "s3://${aws_s3_bucket.artifacts.id}/${aws_s3_object.pipeline_package.key}"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }
}

resource "aws_glue_trigger" "daily" {
  name     = "${local.prefix}-daily"
  type     = "SCHEDULED"
  schedule = var.schedule
  actions {
    job_name = aws_glue_job.clean_events.name
  }
}

# ---------- Alerting ----------
resource "aws_sns_topic" "alerts" {
  name = "${local.prefix}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_rule" "job_failed" {
  name = "${local.prefix}-glue-job-failed"
  event_pattern = jsonencode({
    source        = ["aws.glue"]
    "detail-type" = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.clean_events.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_target" "to_sns" {
  rule = aws_cloudwatch_event_rule.job_failed.name
  arn  = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "sns_allow_events" {
  statement {
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.sns_allow_events.json
}

# ---------- Athena ----------
resource "aws_athena_workgroup" "this" {
  name = "${local.prefix}-wg"
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = var.athena_scan_limit_bytes
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/"
    }
  }
}
