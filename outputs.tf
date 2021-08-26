output "benchmark_eol_api_gateway_id" {
  value = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
}

output "benchmark_eol_api_gateway_satge_name" {
  value = "${aws_api_gateway_stage.dsci_eol_benchmark_stage.stage_name}"
}
