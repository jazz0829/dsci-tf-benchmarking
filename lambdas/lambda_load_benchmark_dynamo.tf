module "dsci_load_benchmark_data_to_dynamodb_lambda" {
  source                      = "../lambda_localfile"
  app_name                    = "${local.dsci_benchmark_load_marketing_data_to_dynamodb_lambda_function_name}"
  description                 = "Lambda function to store kpis into DynamoDB Table for the marketing website"
  iam_policy_document         = "${data.aws_iam_policy_document.lambda_load_benchmark_data_to_dynamodb_iam_policy_document.json}"
  assume_role_policy_document = "${data.aws_iam_policy_document.lambda_assume_role.json}"
  lambda_filename             = "${data.archive_file.load_benchmark_marketing_dynamo_lambda_archive_file.output_path}"
  lambda_source_code_hash     = "${data.archive_file.load_benchmark_marketing_dynamo_lambda_archive_file.output_base64sha256}"
  handler                     = "${var.handler}"
  runtime                     = "${var.runtime}"
  memory_size                 = "${var.memory_size}"

  environment_variables = {
    BENCHMARK_MARKETING_DYNAMO_DB = "${var.dsci_benchmark_marketing_dynamodb_table_name}"
    BENCHMARK_MARKETING_BUCKET    = "${var.dsci_benchmark_bucket_name}"
    BENCHMARK_MARKETING_SECTORS   = "${var.dsci_benchmark_sectors}"
    BENCHMARK_KPIS                = "${var.dsci_benchmark_total_kpis}"
    BENCHMARK_BOOKMARK_DYNAMO_DB  = "${var.dsci_benchmark_bookmark_dynamodb_table_name}"
  }

  alarm_action_arn               = ""
  monitoring_enabled             = 0
  iteratorage_monitoring_enabled = false
  timeout                        = 300
  region                         = "${var.region}"

  tags = "${var.tags}"
}

data "aws_iam_policy_document" "lambda_load_benchmark_data_to_dynamodb_iam_policy_document" {
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
      "s3:Get*",
      "s3:List*",
    ]

    resources = ["arn:aws:s3:::${var.dsci_benchmark_bucket_name}", "arn:aws:s3:::${var.dsci_benchmark_bucket_name}/*"]
  }

  statement {
    effect = "Allow"

    actions = [
      "dynamodb:BatchWriteItem",
      "dynamodb:PutItem",
      "iam:PassRole",
    ]

    resources = [
      "${var.dsci_benchmark_marketing_dynamodb_table_arn}",
      "${var.dsci_benchmark_bookmark_dynamodb_table_arn}",
      "${var.dsci_benchmark_marketing_dynamodb_table_arn}/index/*",
      "${var.dsci_benchmark_bookmark_dynamodb_table_arn}/index/*",
    ]
  }
}

data "null_data_source" "load_benchmark_marketing_dynamo_lambda_file" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-benchmark-data-to-dynamodb/handler.py", length(path.cwd) + 1, -1)}"
  }
}

data "null_data_source" "load_benchmark_marketing_dynamo_lambda_file_archive" {
  inputs {
    filename = "${substr("${path.module}/functions/dsci-benchmark-data-to-dynamodb.zip", length(path.cwd) + 1, -1)}"
  }
}

data "archive_file" "load_benchmark_marketing_dynamo_lambda_archive_file" {
  type        = "zip"
  source_file = "${data.null_data_source.load_benchmark_marketing_dynamo_lambda_file.outputs.filename}"
  output_path = "${data.null_data_source.load_benchmark_marketing_dynamo_lambda_file_archive.outputs.filename}"
}
