resource "aws_lambda_function" "lambda" {
  function_name = "${var.app_name}"
  description   = "${var.description}"
  role          = "${aws_iam_role.iam_for_lambda.arn}"

  filename         = "${var.lambda_filename}"
  source_code_hash = "${var.lambda_source_code_hash}"

  handler     = "${var.handler}"
  runtime     = "${var.runtime}"
  memory_size = "${var.memory_size}"
  timeout     = "${var.timeout}"

  environment {
    variables = "${var.environment_variables}"
  }

  tags = "${var.tags}"
}

resource "aws_iam_role" "iam_for_lambda" {
  name               = "${var.app_name}"
  assume_role_policy = "${var.assume_role_policy_document}"

  tags = "${var.tags}"
}

resource "aws_iam_role_policy" "iam_policy_for_lambda" {
  name   = "${var.app_name}"
  role   = "${aws_iam_role.iam_for_lambda.id}"
  policy = "${var.iam_policy_document}"
}
