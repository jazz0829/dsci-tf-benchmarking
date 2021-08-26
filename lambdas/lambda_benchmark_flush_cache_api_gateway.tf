module "dsci_benchmark_flush_cache_api_gateway" {
  source                      = "../lambda_localfile"
  app_name                    = "${local.dsci_benchmark_flush_cache_api_gateway_lambda_function_name}"
  description                 = "Lambda responsible for flushing the cache from api gateway"
  iam_policy_document         = "${data.aws_iam_policy_document.lambda_benchmark_flush_cache_api_gateway_iam_policy_document.json}"
  assume_role_policy_document = "${data.aws_iam_policy_document.lambda_assume_role.json}"
  lambda_filename             = "${data.archive_file.benchmark_flush_cache_api_gateway_archive_file.output_path}"
  lambda_source_code_hash     = "${data.archive_file.benchmark_flush_cache_api_gateway_archive_file.output_base64sha256}"
  handler                     = "${var.handler}"
  runtime                     = "${var.runtime}"
  memory_size                 = "${var.memory_size}"

  environment_variables = {
    BENCHMARK = "FOO"
  }

  alarm_action_arn               = ""
  monitoring_enabled             = 0
  iteratorage_monitoring_enabled = false
  timeout                        = 300
  region                         = "${var.region}"

  tags = "${var.tags}"
}

data "null_data_source" "benchmark_flush_cache_api_gateway_file" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-benchmark-flush-cache-api-gateway/handler.py", length(path.cwd) + 1, -1)}"
  }
}

data "null_data_source" "benchmark_flush_cache_api_gateway_file_archive" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-benchmark-flush-cache-api-gateway.zip", length(path.cwd) + 1, -1)}"
  }
}

data "archive_file" "benchmark_flush_cache_api_gateway_archive_file" {
  type        = "zip"
  source_file = "${data.null_data_source.benchmark_flush_cache_api_gateway_file.outputs.filename}"
  output_path = "${data.null_data_source.benchmark_flush_cache_api_gateway_file_archive.outputs.filename}"
}

data "aws_iam_policy_document" "lambda_benchmark_flush_cache_api_gateway_iam_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "apigateway:*",
      "iam:PassRole",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = [
      "*",
    ]
  }
}
