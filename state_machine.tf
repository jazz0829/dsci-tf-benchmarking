resource "aws_sfn_state_machine" "sfn_state_machine" {
  name       = "${local.dsci_benchmark_state_machine}"
  role_arn   = "${aws_iam_role.state_machine_role.arn}"
  definition = "${data.template_file.sfn_definition.rendered}"

  tags = "${var.default_tags}"
}

resource "aws_iam_role" "state_machine_role" {
  name               = "${local.dsci_benchmark_state_machine}"
  assume_role_policy = "${data.aws_iam_policy_document.state_machine_assume_role_policy_document.json}"

  tags = "${var.default_tags}"
}

resource "aws_iam_role_policy" "iam_policy_for_state_machine" {
  name   = "${local.dsci_benchmark_state_machine_invoke_policy}"
  role   = "${aws_iam_role.state_machine_role.id}"
  policy = "${data.aws_iam_policy_document.benchmarking_state_machine_iam_policy_document.json}"
}

data "aws_iam_policy_document" "benchmarking_state_machine_iam_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "lambda:InvokeFunction",
    ]

    resources = [
      "arn:aws:lambda:*:*:function:${local.name_prefix}-benchmark-*",
      "arn:aws:lambda:*:*:function:${local.name_prefix}-sagemaker-*",
      "${data.terraform_remote_state.base_config.lambda_update_dynamodb_capacity_arn}",
    ]
  }
}
