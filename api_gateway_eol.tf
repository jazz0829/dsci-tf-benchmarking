resource "aws_api_gateway_rest_api" "dsci_benchmark_eol_api" {
  name = "${local.dsci_benchmark_eol_api_gateway}"
}

resource "aws_api_gateway_resource" "dsci_benchmark_eol_resource" {
  path_part   = "${var.benchmark_eol_resource_name}"
  parent_id   = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.root_resource_id}"
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
}

resource "aws_api_gateway_method" "dsci_benchmark_eol_method" {
  rest_api_id      = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id      = "${aws_api_gateway_resource.dsci_benchmark_eol_resource.id}"
  http_method      = "GET"
  authorization    = "NONE"
  api_key_required = true
}

resource "aws_api_gateway_integration" "dsci_benchmark_eol_integration" {
  rest_api_id             = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id             = "${aws_api_gateway_resource.dsci_benchmark_eol_resource.id}"
  http_method             = "${aws_api_gateway_method.dsci_benchmark_eol_method.http_method}"
  integration_http_method = "POST"
  type                    = "AWS"
  uri                     = "arn:aws:apigateway:${var.region}:lambda:path/2015-03-31/functions/${module.dsci_benchmark_marketing_lambdas.dsci_benchmark_eol_scan_dynamodb_lambda_arn}/invocations"
}

resource "aws_lambda_permission" "dsci_benchmark_eol_api_gateway_lambda_permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = "${module.dsci_benchmark_marketing_lambdas.dsci_benchmark_eol_scan_dynamo_db_lambda_name}"
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.execution_arn}/*/*/*"
}

resource "aws_api_gateway_method_response" "dsci_benchmark_eol_method_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_eol_resource.id}"
  http_method = "${aws_api_gateway_method.dsci_benchmark_eol_method.http_method}"
  status_code = "200"

  response_models = {
    "application/json" = "Empty"
  }

  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = ""
  }
}

resource "aws_api_gateway_integration_response" "dsci_benchmark_eol_integration_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_eol_resource.id}"
  http_method = "${aws_api_gateway_method.dsci_benchmark_eol_method.http_method}"

  status_code = "${aws_api_gateway_method_response.dsci_benchmark_eol_method_response.status_code}"

  response_templates = {
    "application/json" = ""
  }

  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }

  depends_on = [
    "aws_api_gateway_integration.dsci_benchmark_eol_integration",
  ]
}

resource "aws_api_gateway_deployment" "dsci_benchmark_eol_api_deployment" {
  depends_on = [
    "aws_api_gateway_integration.dsci_benchmark_eol_integration",
  ]

  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"

  variables {
    deployed_at = "${timestamp()}"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_api_key" "dsci_benchmark_eol_api_gateway_key" {
  name        = "${local.dsci_benchmark_eol_api_gateway_key}"
  description = "Api Key created for accessing the method the kpis for EOL"
}

resource "aws_api_gateway_usage_plan" "dsci_benchmark_eol_usage_plan" {
  name        = "${local.dsci_benchmark_eol_api_gateway_usage_plan}"
  description = "Used for EOL Benchmarking api"

  api_stages {
    api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
    stage  = "${aws_api_gateway_stage.dsci_eol_benchmark_stage.stage_name}"
  }

  throttle_settings {
    burst_limit = 1000
    rate_limit  = 1000
  }
}

resource "aws_api_gateway_usage_plan_key" "benchmark_eol_api_gateway_usage_plan_key" {
  key_id        = "${aws_api_gateway_api_key.dsci_benchmark_eol_api_gateway_key.id}"
  key_type      = "API_KEY"
  usage_plan_id = "${aws_api_gateway_usage_plan.dsci_benchmark_eol_usage_plan.id}"
}

resource "aws_api_gateway_method" "eol_cors_method" {
  rest_api_id   = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id   = "${aws_api_gateway_resource.dsci_benchmark_eol_resource.id}"
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "eol_cors_integration" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_eol_resource.id}"
  http_method = "${aws_api_gateway_method.eol_cors_method.http_method}"
  type        = "MOCK"

  request_templates = {
    "application/json" = <<EOF
  { "statusCode": 200 }
  EOF
  }
}

resource "aws_api_gateway_method_response" "eol_cors_method_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_eol_resource.id}"
  http_method = "${aws_api_gateway_method.eol_cors_method.http_method}"

  status_code = "200"

  response_models = {
    "application/json" = "Empty"
  }

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = false
    "method.response.header.Access-Control-Allow-Methods" = false
    "method.response.header.Access-Control-Allow-Origin"  = false
  }
}

resource "aws_api_gateway_integration_response" "eol_cors_integration_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  resource_id = "${aws_api_gateway_method.eol_cors_method.resource_id}"
  http_method = "${aws_api_gateway_method.eol_cors_method.http_method}"

  status_code = "${aws_api_gateway_method_response.eol_cors_method_response.status_code}"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }
}

resource "aws_api_gateway_stage" "dsci_eol_benchmark_stage" {
  depends_on            = ["aws_api_gateway_deployment.dsci_benchmark_eol_api_deployment"]
  stage_name            = "${local.dsci_eol_benchmark_stage_name}"
  rest_api_id           = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  deployment_id         = "${aws_api_gateway_deployment.dsci_benchmark_eol_api_deployment.id}"
  cache_cluster_enabled = true
  cache_cluster_size    = 0.5
}

resource "aws_api_gateway_method_settings" "eol_api_general_settings" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  stage_name  = "${aws_api_gateway_stage.dsci_eol_benchmark_stage.stage_name}"
  method_path = "*/*"

  settings {
    # Enable CloudWatch logging and metrics
    metrics_enabled      = true
    data_trace_enabled   = true
    logging_level        = "INFO"
    caching_enabled      = true
    cache_ttl_in_seconds = "${var.max_cache_ttl_in_seconds}"
  }
}
