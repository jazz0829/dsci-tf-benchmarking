output "dsci_load_marketing_benchmark_data_to_dynamodb_lambda_arn" {
  value = "${module.dsci_load_benchmark_data_to_dynamodb_lambda.lambda_arn}"
}

output "dsci_load_eol_benchmark_data_to_dynamodb_lambda_arn" {
  value = "${module.dsci_load_eol_benchmark_data_to_dynamodb_lambda.lambda_arn}"
}

output "dsci_benchmark_eol_scan_dynamodb_lambda_arn" {
  value = "${module.dsci_benchmark_eol_scan_dynamodb.lambda_arn}"
}

output "dsci_benchmark_eol_scan_dynamo_db_lambda_name" {
  value = "${module.dsci_benchmark_eol_scan_dynamodb.lambda_name}"
}

output "dsci_benchmark_marketing_query_dynamo_db_lambda_name" {
  value = "${module.dsci_benchmark_marketing_query_dynamodb.lambda_name}"
}

output "dsci_benchmark_marketing_query_dynamo_db_lambda_arn" {
  value = "${module.dsci_benchmark_marketing_query_dynamodb.lambda_arn}"
}

output "dsci_benchmark_marketing_query_dynamo_db_lambda_invoke_arn" {
  value = "${module.dsci_benchmark_marketing_query_dynamodb.invocation_arn}"
}

output "dsci_flush_cache_api_gateway_lambda_arn" {
  value = "${module.dsci_benchmark_flush_cache_api_gateway.lambda_arn}"
}

output "dsci_invoke_benchmark_api_lambda_arn" {
  value = "${module.dsci_invoke_benchmark_api_lambda.lambda_arn}"
}
