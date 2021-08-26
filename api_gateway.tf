resource "aws_api_gateway_rest_api" "dsci_benchmark_marketing_api" {
  name = "${local.dsci_benchmark_marketing_api_gateway_name}"

  policy = <<EOF
  {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Effect": "Allow",
              "Principal": "*",
              "Action": "execute-api:Invoke",
              "Resource": "arn:aws:execute-api:eu-west-1:${var.accountid}:*/*/*",
              "Condition": {
                  "IpAddress": {"aws:SourceIp": ${jsonencode(var.allowed_ip_addresses)}}
              }
          }
      ]
  }
  EOF
}

resource "aws_api_gateway_resource" "dsci_benchmark_marketing_resource" {
  path_part   = "${var.dsci_benchmark_marketing_resource_name}"
  parent_id   = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.root_resource_id}"
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
}

resource "aws_api_gateway_request_validator" "dsci_benchmark_request_validator" {
  name                        = "${var.request_validator}"
  rest_api_id                 = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  validate_request_body       = false
  validate_request_parameters = true
}

resource "aws_api_gateway_method" "dsci_benchmark_marketing_method" {
  rest_api_id          = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id          = "${aws_api_gateway_resource.dsci_benchmark_marketing_resource.id}"
  http_method          = "GET"
  authorization        = "NONE"
  api_key_required     = true
  request_validator_id = "${aws_api_gateway_request_validator.dsci_benchmark_request_validator.id}"

  request_parameters = {
    "method.request.querystring.kpi"        = true
    "method.request.querystring.sectorcode" = true
    "method.request.querystring.history"    = false
  }
}

resource "aws_api_gateway_integration" "dsci_benchmark_marketing_integration" {
  rest_api_id             = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id             = "${aws_api_gateway_resource.dsci_benchmark_marketing_resource.id}"
  http_method             = "${aws_api_gateway_method.dsci_benchmark_marketing_method.http_method}"
  integration_http_method = "POST"
  type                    = "AWS"
  cache_key_parameters    = ["method.request.querystring.kpi", "method.request.querystring.sectorcode", "method.request.querystring.history"]
  cache_namespace         = "dsci-benchmark"
  uri                     = "${module.dsci_benchmark_marketing_lambdas.dsci_benchmark_marketing_query_dynamo_db_lambda_invoke_arn}"

  request_templates = {
    "application/json" = <<EOF
  {
    "sectorcode": "$input.params().querystring.get('sectorcode')",
    "kpi": "$input.params().querystring.get('kpi')",
    "history": "$input.params().querystring.get('history')"
  }
  EOF
  }
}

resource "aws_lambda_permission" "dsci_benchmark_marketing_api_gateway_lambda_permission" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = "${module.dsci_benchmark_marketing_lambdas.dsci_benchmark_marketing_query_dynamo_db_lambda_name}"
  principal     = "apigateway.amazonaws.com"

  source_arn = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.execution_arn}/*/*/*"
}

resource "aws_api_gateway_method_response" "dsci_benchmark_marketing_lambda_api_method_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_marketing_resource.id}"
  http_method = "${aws_api_gateway_method.dsci_benchmark_marketing_method.http_method}"
  status_code = "200"

  response_models = {
    "application/json" = "Empty"
  }

  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = ""
  }
}

resource "aws_api_gateway_integration_response" "dsci_benchmark_marketing_lambda_api_integration_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_marketing_resource.id}"
  http_method = "${aws_api_gateway_method.dsci_benchmark_marketing_method.http_method}"

  status_code = "${aws_api_gateway_method_response.dsci_benchmark_marketing_lambda_api_method_response.status_code}"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }

  depends_on = [
    "aws_api_gateway_integration.dsci_benchmark_marketing_integration",
  ]
}

resource "aws_api_gateway_deployment" "dsci_benchmark_marketing_api_deployment" {
  depends_on = [
    "aws_api_gateway_integration.dsci_benchmark_marketing_integration",
  ]

  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"

  variables {
    deployed_at = "${timestamp()}"
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_api_key" "dsci_benchmark_marketing_api_gateway_key" {
  name        = "${local.dsci_benchmark_marketing_api_gateway_key}"
  description = "Api Key created for accessing the method the kpis for the marketing page"
}

resource "aws_api_gateway_usage_plan" "dsci_benchmark_marketing_usage_plan" {
  name        = "${local.dsci_benchmark_marketing_api_gateway_key}"
  description = "Used for benchmark marketing api"

  api_stages {
    api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
    stage  = "${aws_api_gateway_stage.dsci_benchmark_stage.stage_name}"
  }

  throttle_settings {
    burst_limit = 1000
    rate_limit  = 1000
  }
}

resource "aws_api_gateway_usage_plan_key" "benchmark_marketing_api_gateway_usage_plan_key" {
  key_id        = "${aws_api_gateway_api_key.dsci_benchmark_marketing_api_gateway_key.id}"
  key_type      = "API_KEY"
  usage_plan_id = "${aws_api_gateway_usage_plan.dsci_benchmark_marketing_usage_plan.id}"
}

resource "aws_api_gateway_method" "cors_method" {
  rest_api_id   = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id   = "${aws_api_gateway_resource.dsci_benchmark_marketing_resource.id}"
  http_method   = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "cors_integration" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_marketing_resource.id}"
  http_method = "${aws_api_gateway_method.cors_method.http_method}"
  type        = "MOCK"

  request_templates = {
    "application/json" = <<EOF
  { "statusCode": 200 }
  EOF
  }
}

resource "aws_api_gateway_method_response" "cors_method_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id = "${aws_api_gateway_resource.dsci_benchmark_marketing_resource.id}"
  http_method = "${aws_api_gateway_method.cors_method.http_method}"

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

resource "aws_api_gateway_integration_response" "cors_integration_response" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  resource_id = "${aws_api_gateway_method.cors_method.resource_id}"
  http_method = "${aws_api_gateway_method.cors_method.http_method}"

  status_code = "${aws_api_gateway_method_response.cors_method_response.status_code}"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }
}

resource "aws_api_gateway_stage" "dsci_benchmark_stage" {
  depends_on            = ["aws_api_gateway_deployment.dsci_benchmark_marketing_api_deployment"]
  stage_name            = "${local.dsci_marketing_benchmark_stage_name}"
  rest_api_id           = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  deployment_id         = "${aws_api_gateway_deployment.dsci_benchmark_marketing_api_deployment.id}"
  cache_cluster_enabled = true
  cache_cluster_size    = 0.5
}

resource "aws_api_gateway_method_settings" "general_settings" {
  rest_api_id = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  stage_name  = "${aws_api_gateway_stage.dsci_benchmark_stage.stage_name}"
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
