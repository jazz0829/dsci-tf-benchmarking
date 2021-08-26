resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/${var.app_name}"
  retention_in_days = "${var.log_group_retention_days}"

  tags = "${var.tags}"
}

resource "aws_cloudwatch_log_metric_filter" "lambda_memory_metric" {
  count = "${var.cloudwatch_monitoring_enabled}"

  name           = "${var.app_name}_memory_metric_filter"
  pattern        = "${var.pattern}"
  log_group_name = "${var.log_group_name}"

  metric_transformation {
    name      = "${var.app_name}-memory-used"
    namespace = "CIG"
    value     = "$MemoryUsed"
  }

  depends_on = ["aws_cloudwatch_log_group.lambda_log_group"]
}

resource "aws_cloudwatch_metric_alarm" "memory_monitoring_metric" {
  count = "${var.cloudwatch_monitoring_enabled}"

  alarm_name                = "${var.app_name}_lambda_memory_alarm"
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  evaluation_periods        = "1"
  datapoints_to_alarm       = "1"
  metric_name               = "${var.app_name}-memory-used"
  namespace                 = "CIG"
  period                    = "${var.default_period}"
  statistic                 = "Maximum"
  threshold                 = "${var.cloudwatch_alarm_threshold}"
  alarm_description         = "Alert on memory usage of lambda function ${var.app_name}"
  insufficient_data_actions = []

  alarm_actions = [
    "${var.cloudwatch_alarm_action_arn}",
  ]

  ok_actions         = ["${var.ok_action_arn}"]
  actions_enabled    = "${var.alarm_action_enabled}"
  treat_missing_data = "notBreaching"

  tags = "${var.tags}"
}

resource "aws_cloudwatch_metric_alarm" "error_monitoring_metric" {
  count = "${var.cloudwatch_monitoring_enabled}"

  alarm_name          = "${var.app_name}_lambda_error_alarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "${var.default_period}"
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alert on errors in lambda function ${var.app_name}"

  alarm_actions = [
    "${var.cloudwatch_alarm_action_arn}",
  ]

  ok_actions = ["${var.ok_action_arn}"]

  dimensions {
    FunctionName = "${var.app_name}"
  }

  treat_missing_data = "notBreaching"

  tags = "${var.tags}"
}

resource "aws_cloudwatch_metric_alarm" "streamalert_lambda_iterator_age" {
  count = "${var.cloudwatch_monitoring_enabled && var.iterator_age_alarm_enabled ? 1 : 0}"

  alarm_name          = "${var.app_name}_iterator_age_alarm"
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  statistic           = "Maximum"
  comparison_operator = "GreaterThanThreshold"
  threshold           = "${var.iterator_age_alarm_threshold_ms}"
  evaluation_periods  = 1
  period              = "${var.iterator_age_alarm_period_secs}"
  alarm_description   = "Lambda High Iterator Age: ${var.app_name}"

  alarm_actions = [
    "${var.cloudwatch_alarm_action_arn}",
  ]

  ok_actions = ["${var.ok_action_arn}"]

  dimensions {
    FunctionName = "${var.app_name}"
  }

  treat_missing_data = "notBreaching"

  tags = "${var.tags}"
}
