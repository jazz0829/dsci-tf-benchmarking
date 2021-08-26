module "lambda_app_cloudwatch" {
  source                          = "../cloudwatch_lambda_monitoring"
  app_name                        = "${var.app_name}"
  log_group_name                  = "/aws/lambda/${var.app_name}"
  cloudwatch_alarm_threshold      = "${var.memory_alert_threshold}"
  cloudwatch_alarm_action_arn     = "${var.alarm_action_arn}"
  ok_action_arn                   = "${var.ok_action_arn}"
  log_group_retention_days        = "${var.log_group_retention_days}"
  cloudwatch_monitoring_enabled   = "${var.monitoring_enabled}"
  iterator_age_alarm_enabled      = "${var.iteratorage_monitoring_enabled}"
  iterator_age_alarm_threshold_ms = "${var.iteratorage_threshold_ms}"
  region                          = "${var.region}"

  tags = "${var.tags}"
}
