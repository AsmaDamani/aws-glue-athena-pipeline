variable "project" {
  description = "Short project name used in resource names"
  type        = string
  default     = "events-pipeline"
}

variable "env" {
  description = "Environment name, e.g. dev or prod"
  type        = string
  default     = "dev"
}

variable "region" {
  type    = string
  default = "ap-southeast-2"
}

variable "worker_type" {
  description = "Glue worker type. G.1X suits most small-to-medium jobs."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  type    = number
  default = 2
}

variable "schedule" {
  description = "Glue cron expression (UTC)"
  type        = string
  default     = "cron(0 19 * * ? *)"
}

variable "alert_email" {
  description = "Email to notify when the job fails (leave empty to skip)"
  type        = string
  default     = ""
}

variable "athena_scan_limit_bytes" {
  description = "Per-query scan cap to stop runaway Athena costs"
  type        = number
  default     = 10737418240 # 10 GB
}
