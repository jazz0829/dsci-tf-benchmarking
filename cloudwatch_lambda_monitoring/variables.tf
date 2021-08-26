variable "region" {}

variable "app_name" {}

variable "tags" {
  type    = "map"
  default = {}
}

variable "default_period" {
  default = "60"
}

variable "log_group_name" {}

variable "pattern" {
  default = "[type=REPORT,...,MemoryUsed,MemoryUnit]"
}

variable "alarm_action_enabled" {
  default = "true"
}

variable "cloudwatch_alarm_threshold" {}

variable "cloudwatch_alarm_action_arn" {}
variable "ok_action_arn" {}

variable "log_group_retention_days" {
  default = 30
}

variable "cloudwatch_monitoring_enabled" {
  default = 1
}

variable "iterator_age_alarm_enabled" {
  default     = false
  description = "Enable IteratorAge alarm (applicable only for stream-based invocations like Kinesis)"
}

variable "iterator_age_alarm_threshold_ms" {
  default     = 600000
  description = "Alarm if the Lambda IteratorAge (ms) exceeds this value in the specified period(s)"
}

variable "iterator_age_alarm_period_secs" {
  default     = 120
  description = "Period over which to evaluate the maximum IteratorAge"
}
