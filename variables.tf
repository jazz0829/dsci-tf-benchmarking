variable "region" {}

variable "accountid" {}

variable "handler" {
  default = "handler.lambda_handler"
}

variable "dynamodb_table_hash_key" {
  default = "kpi_identifier"
}

variable "default_tags" {
  type        = "map"
  description = "Table to store results of kpis to be exposed to exact marketing page"

  default = {
    Terraform   = "true"
    GitHub-Repo = "exactsoftware/dsci-tf-benchmarking"
    Project     = "Benchmark-Marketing"
  }
}

#Variables for step function
variable "step_function_definition_file" {
  default = "step-function.json"
}

variable "emr_master_node_instance_type" {
  default = "m5.xlarge"
}

variable "emr_master_node_instance_count" {
  default = 1
}

variable "emr_core_node_instance_type" {
  default = "r4.2xlarge"
}

variable "emr_core_node_instance_count" {
  default = 12
}

variable "emr_data_collection_input_location" {
  default = "s3://dsci-eol-data-bucket/prod/domain/"
}

#event-rule

variable "start_benchmark_schedule" {
  default = "cron(0 3 ? * 2#1 *)"
}

variable "start_benchmark_event_rule_description" {
  default = "Cloudwatch event rule to schedule step function for benchmark"
}

variable "benchmark_event_rule_enabled" {}

variable "start_benchmark_event_target_id" {
  default = "StartBenchmarkSteps"
}

variable "start_benchmark_event_input_file" {
  default = "cloudwatch-event-rules/dsci-benchmark/input.json"
}

variable "allowed_ip_addresses" {
  type = "list"

  default = [
    "145.14.1.3",
    "46.31.188.128/8",
  ]
}

variable "exact_ip_address" {
  default = "145.14.1.3"
}

variable "benchmark_eol_resource_name" {
  default = "Benchmarking"
}

variable "request_validator" {
  default = "dsci-benchmark-request-validator"
}

variable "api_version" {
  default = "v1"
}

variable "high_write_capacity_units" {
  default = 50
}

variable "low_write_capacity_units" {
  default = 1
}

variable "max_cache_ttl_in_seconds" {
  default = 3600
}

variable "dsci_benchmark_marketing_resource_name" {
  default = "marketing"
}

variable "ec2_subnet_id" {}
