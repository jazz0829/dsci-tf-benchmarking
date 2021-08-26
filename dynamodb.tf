resource "aws_dynamodb_table" "dsci_benchmark_marketing" {
  name           = "${local.dsci_benchmark_marketing_dynamodb_table_name}"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 1
  hash_key       = "${var.dynamodb_table_hash_key}"

  attribute {
    name = "${var.dynamodb_table_hash_key}"
    type = "S"
  }

  tags = "${var.default_tags}"
}

resource "aws_dynamodb_table" "dynamodb_benchmark_bookmark_table" {
  name           = "${local.dsci_benchmark_bookmark_dynamodb_table_name}"
  billing_mode   = "PROVISIONED"
  read_capacity  = 1
  write_capacity = 1
  hash_key       = "S3Key"

  attribute {
    name = "S3Key"
    type = "S"
  }

  tags = "${var.default_tags}"
}

resource "aws_dynamodb_table" "dsci_benchmark_eol" {
  name           = "${local.dsci_benchmark_eol_dynamodb_table_name}"
  billing_mode   = "PROVISIONED"
  read_capacity  = 5
  write_capacity = 1
  hash_key       = "${var.dynamodb_table_hash_key}"

  attribute {
    name = "${var.dynamodb_table_hash_key}"
    type = "S"
  }

  tags = "${var.default_tags}"
}
