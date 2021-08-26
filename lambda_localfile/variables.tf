variable "region" {}

variable "app_name" {}

variable "description" {}

variable "iam_policy_document" {}

variable "assume_role_policy_document" {}

variable "environment_variables" {
  type    = "map"
  default = {}
}

variable "tags" {
  type    = "map"
  default = {}
}

variable "runtime" {
  default = "python3.6"
}

variable "handler" {
  default = "lambda_function.lambda_handler"
}

variable "log_group_retention_days" {
  default = 30
}

variable "memory_size" {
  default = "128"
}

variable "memory_alert_threshold" {
  default = "120"
}

variable "alarm_action_arn" {
  default = ""
}

variable "ok_action_arn" {
  default = ""
}

variable "dead_letter_config" {
  default = ""
}

variable "timeout" {
  default = "3"
}

variable "monitoring_enabled" {
  default = 1
}

variable "iteratorage_monitoring_enabled" {
  default = false
}

variable "iteratorage_threshold_ms" {
  default = 600000
}

variable "lambda_filename" {}

variable "lambda_source_code_hash" {}
