locals {
  dsci_benchmark_load_marketing_data_to_dynamodb_lambda_function_name = "${var.name_prefix}-benchmark-load-marketing-data-to-dynamodb"
  dsci_benchmark_roll_back_lambda_function_name                       = "${var.name_prefix}-benchmark-roll-back"
  dsci_benchmark_load_eol_data_to_dynamodb_lambda_function_name       = "${var.name_prefix}-benchmark-load-eol-data-to-dynamodb"
  dsci_benchmark_eol_roll_back_lambda_function_name                   = "${var.name_prefix}-benchmark-eol-roll-back"
  dsci_benchmark_eol_scan_dynamodb_lambda_function_name               = "${var.name_prefix}-benchmark-eol-scan-dynamodb"
  dsci_benchmark_marketing_query_dynamodb_lambda_function_name        = "${var.name_prefix}-benchmark-marketing-query-dynamodb"
  dsci_benchmark_flush_cache_api_gateway_lambda_function_name         = "${var.name_prefix}-benchmark-flush-cache-api-gateway"
  dsci_benchmark_invoke_api_lambda_function_name                      = "${var.name_prefix}-benchmark-invoke-api"
}
