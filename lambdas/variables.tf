variable "name_prefix" {}
variable "region" {}

variable "tags" {
  type    = "map"
  default = {}
}

variable "handler" {
  default = "handler.lambda_handler"
}

variable "runtime" {
  default = "python3.7"
}

variable "memory_size" {
  default = 512
}

variable "dsci_benchmark_sectors" {
  default = "C,G,M69,I,N,F,ALL"
}

variable "dsci_benchmark_total_kpis" {
  default = 4
}

variable "dsci_benchmark_bucket_name" {}
variable "dsci_benchmark_marketing_dynamodb_table_name" {}
variable "dsci_benchmark_marketing_dynamodb_table_arn" {}
variable "dsci_benchmark_eol_dynamodb_table_name" {}
variable "dsci_benchmark_eol_dynamodb_table_arn" {}
variable "dsci_benchmark_bookmark_dynamodb_table_name" {}
variable "dsci_benchmark_bookmark_dynamodb_table_arn" {}
variable "dsci_benchmark_eol_api_id" {}
variable "dsci_benchmark_eol_api_stage_name" {}
variable "dsci_benchmark_marketing_api_id" {}
variable "dsci_benchmark_marketing_api_stage_name" {}
variable "marketing_apigateway_rest_api_id" {}
variable "marketing_apigateway_stage_name" {}
variable "marketing_apigateway_resource_name" {}
variable "marketing_apigateway_api_key" {}
variable "eol_apigateway_rest_api_id" {}
variable "eol_apigateway_stage_name" {}
variable "eol_apigateway_resource_name" {}
variable "eol_apigateway_api_key" {}
