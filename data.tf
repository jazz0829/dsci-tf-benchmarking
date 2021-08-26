data "template_file" "sfn_definition" {
  template = "${file(var.step_function_definition_file)}"

  vars = {
    dsci-benchmark-marketing-lambda-dynamo-arn = "${module.dsci_benchmark_marketing_lambdas.dsci_load_marketing_benchmark_data_to_dynamodb_lambda_arn}"
    dsci-benchmark-eol-lambda-dynamo-arn       = "${module.dsci_benchmark_marketing_lambdas.dsci_load_eol_benchmark_data_to_dynamodb_lambda_arn}"
    dsci-benchmark-run-emr-job-arn             = "${data.terraform_remote_state.base_config.lambda_app_run_emr_job_arn}"
    dsci-sagemaker-slack-lambda-arn            = "${data.terraform_remote_state.base_config.lambda_app_notify_slack_arn}"
    dsci-sagemaker-emr-get-status-lambda-arn   = "${data.terraform_remote_state.base_config.lambda_app_get_emr_cluster_status_arn}"
    dsci-update-dynamodb-capacity-lambda-arn   = "${data.terraform_remote_state.base_config.lambda_update_dynamodb_capacity_arn}"
    dsci-flush-cache-api-gateway               = "${module.dsci_benchmark_marketing_lambdas.dsci_flush_cache_api_gateway_lambda_arn}"
    dsci-benchmark-invoke-api                  = "${module.dsci_benchmark_marketing_lambdas.dsci_invoke_benchmark_api_lambda_arn}"
  }
}

data "template_file" "input_definition" {
  template = "${file("input.json")}"

  vars = {
    emr_benchmark_raw_script             = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.benchmark_raw.id}"
    emr_benchmark_division_script        = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.benchmark_division.id}"
    emr_benchmark_aggregation_script     = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.benchmark_aggregation.id}"
    emr_benchmark_elements_script        = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.benchmark_elements.id}"
    emr_benchmark_eol_division_script    = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.benchmark_eol_division.id}"
    emr_benchmark_eol_aggregation_script = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.benchmark_eol_aggregation.id}"
    emr_log_output                       = "s3://${local.dsci_benchmark_bucket_name}/emr-logs/"
    emr_install_dependencies_script      = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.install_dependencies.id}"
    emr_master_node_instance_type        = "${var.emr_master_node_instance_type}"
    emr_master_node_instance_count       = "${var.emr_master_node_instance_count}"
    emr_core_node_instance_type          = "${var.emr_core_node_instance_type}"
    emr_core_node_instance_count         = "${var.emr_core_node_instance_count}"
    rgs1-1-csv-file                      = "s3://${local.dsci_benchmark_bucket_name}/${aws_s3_bucket_object.rgs1-1-csv.id}"
    dsci_benchmark_bucket_name           = "${local.dsci_benchmark_bucket_name}"
    dynamodb_eol_table_name              = "${local.dsci_benchmark_eol_dynamodb_table_name}"
    dynamodb_marketing_table_name        = "${local.dsci_benchmark_marketing_dynamodb_table_name}"
    low_write_capacity_units             = "${var.low_write_capacity_units}"
    high_write_capacity_units            = "${var.high_write_capacity_units}"
    api_id_marketing                     = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
    stage_marketing                      = "${aws_api_gateway_stage.dsci_benchmark_stage.stage_name}"
    api_id_eol                           = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
    stage_eol                            = "${aws_api_gateway_stage.dsci_eol_benchmark_stage.stage_name}"
    ec2_subnet_id                        = "${var.ec2_subnet_id}"
  }
}

data "aws_iam_policy_document" "state_machine_assume_role_policy_document" {
  statement {
    effect = "Allow"

    actions = [
      "sts:AssumeRole",
    ]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}
