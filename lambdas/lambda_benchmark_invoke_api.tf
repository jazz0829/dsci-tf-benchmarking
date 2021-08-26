module "dsci_invoke_benchmark_api_lambda" {
  source                      = "../lambda_localfile"
  app_name                    = "${local.dsci_benchmark_invoke_api_lambda_function_name}"
  description                 = "Integration test for the Benchmark API (Marketing & EOL)"
  iam_policy_document         = "${data.aws_iam_policy_document.lambda_invoke_benchmark_api_iam_policy_document.json}"
  assume_role_policy_document = "${data.aws_iam_policy_document.lambda_assume_role.json}"
  lambda_filename             = "${data.archive_file.invoke_benchmark_api_lambda_archive_file.output_path}"
  lambda_source_code_hash     = "${data.archive_file.invoke_benchmark_api_lambda_archive_file.output_base64sha256}"
  handler                     = "${var.handler}"
  runtime                     = "${var.runtime}"
  memory_size                 = "${var.memory_size}"

  environment_variables = {
    MARKETING_APIGATEWAY_REST_API_ID   = "${var.marketing_apigateway_rest_api_id}"
    MARKETING_APIGATEWAY_STAGE_NAME    = "${var.marketing_apigateway_stage_name}"
    MARKETING_APIGATEWAY_REGION        = "${var.region}"
    MARKETING_APIGATEWAY_RESOURCE_NAME = "${var.marketing_apigateway_resource_name}"
    MARKETING_APIGATEWAY_API_KEY       = "${var.marketing_apigateway_api_key}"
    EOL_APIGATEWAY_REST_API_ID         = "${var.eol_apigateway_rest_api_id}"
    EOL_APIGATEWAY_STAGE_NAME          = "${var.eol_apigateway_stage_name}"
    EOL_APIGATEWAY_RESOURCE_NAME       = "${var.eol_apigateway_resource_name}"
    EOL_APIGATEWAY_API_KEY             = "${var.eol_apigateway_api_key}"
  }

  alarm_action_arn               = ""
  monitoring_enabled             = 0
  iteratorage_monitoring_enabled = false
  timeout                        = 300
  region                         = "${var.region}"

  tags = "${var.tags}"
}

data "aws_iam_policy_document" "lambda_invoke_benchmark_api_iam_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "arn:aws:logs:*:*:*",
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "apigateway:*",
      "iam:PassRole",
    ]

    resources = [
      "*",
    ]
  }
}

data "null_data_source" "invoke_benchmark_api_lambda_file" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-benchmark-invoke-api/handler.py", length(path.cwd) + 1, -1)}"
  }
}

data "null_data_source" "invoke_benchmark_api_lambda_file_archive" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-benchmark-invoke-api.zip", length(path.cwd) + 1, -1)}"
  }
}

data "archive_file" "invoke_benchmark_api_lambda_archive_file" {
  type        = "zip"
  source_file = "${data.null_data_source.invoke_benchmark_api_lambda_file.outputs.filename}"
  output_path = "${data.null_data_source.invoke_benchmark_api_lambda_file_archive.outputs.filename}"
}
