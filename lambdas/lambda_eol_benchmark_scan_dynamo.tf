module "dsci_benchmark_eol_scan_dynamodb" {
  source                      = "../lambda_localfile"
  app_name                    = "${local.dsci_benchmark_eol_scan_dynamodb_lambda_function_name}"
  description                 = "Returns all the items in the EOL Benchmark DynamoDB table"
  iam_policy_document         = "${data.aws_iam_policy_document.lambda_benchmark_eol_scan_dynamodb_iam_policy_document.json}"
  assume_role_policy_document = "${data.aws_iam_policy_document.lambda_assume_role.json}"
  lambda_filename             = "${data.archive_file.benchmark_eol_scan_dynamodb_archive_file.output_path}"
  lambda_source_code_hash     = "${data.archive_file.benchmark_eol_scan_dynamodb_archive_file.output_base64sha256}"
  handler                     = "${var.handler}"
  runtime                     = "${var.runtime}"
  memory_size                 = "${var.memory_size}"

  environment_variables = {
    BENCHMARK_EOL_DYNAMO_DB = "${var.dsci_benchmark_eol_dynamodb_table_name}"
  }

  alarm_action_arn               = ""
  monitoring_enabled             = 0
  iteratorage_monitoring_enabled = false
  timeout                        = 300
  region                         = "${var.region}"

  tags = "${var.tags}"
}

data "null_data_source" "benchmark_eol_scan_dynamodb_file" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-eol-benchmark-scan-dynamodb/handler.py", length(path.cwd) + 1, -1)}"
  }
}

data "null_data_source" "benchmark_eol_scan_dynamodb_file_archive" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-eol-benchmark-scan-dynamodb.zip", length(path.cwd) + 1, -1)}"
  }
}

data "archive_file" "benchmark_eol_scan_dynamodb_archive_file" {
  type        = "zip"
  source_file = "${data.null_data_source.benchmark_eol_scan_dynamodb_file.outputs.filename}"
  output_path = "${data.null_data_source.benchmark_eol_scan_dynamodb_file_archive.outputs.filename}"
}

data "aws_iam_policy_document" "lambda_benchmark_eol_scan_dynamodb_iam_policy_document" {
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
      "dynamodb:Scan",
      "iam:PassRole",
    ]

    resources = ["${var.dsci_benchmark_eol_dynamodb_table_arn}", "${var.dsci_benchmark_eol_dynamodb_table_arn}/index/*"]
  }
}
