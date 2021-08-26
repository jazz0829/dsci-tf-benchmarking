resource "aws_cloudwatch_event_rule" "cloudwatch_start_benchmark_event_rule" {
  name                = "${local.start_benchmark_event_rule_name}"
  schedule_expression = "${var.start_benchmark_schedule}"
  description         = "${var.start_benchmark_event_rule_description}"
  is_enabled          = "${var.benchmark_event_rule_enabled}"

  tags = "${var.default_tags}"
}

resource "aws_cloudwatch_event_target" "start_benchmark_event_target" {
  target_id = "${var.start_benchmark_event_target_id}"
  rule      = "${aws_cloudwatch_event_rule.cloudwatch_start_benchmark_event_rule.name}"
  arn       = "${aws_sfn_state_machine.sfn_state_machine.id}"
  role_arn  = "${aws_iam_role.cloudwatch_start_benchmark_job_role.arn}"
  input     = "${data.template_file.input_definition.rendered}"
}

resource "aws_iam_role" "cloudwatch_start_benchmark_job_role" {
  name               = "${local.start_benchmark_role_name}"
  assume_role_policy = "${data.aws_iam_policy_document.cloudwatch_assume_role_policy_document.json}"

  tags = "${var.default_tags}"
}

resource "aws_iam_role_policy" "cloudwatch_start_benchmark_job_policy" {
  name   = "${local.start_benchmark_policy_name}"
  role   = "${aws_iam_role.cloudwatch_start_benchmark_job_role.id}"
  policy = "${data.aws_iam_policy_document.cloudwatch_start_execution_policy_document.json}"
}

data "aws_iam_policy_document" "cloudwatch_assume_role_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "cloudwatch_start_execution_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "states:StartExecution",
    ]

    resources = [
      "${aws_sfn_state_machine.sfn_state_machine.id}",
    ]
  }
}
