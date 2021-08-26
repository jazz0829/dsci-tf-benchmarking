# create domain (hosted zone) - might not be needed to create, but to be used (when creating domain, also a hosted zone is created)
data "aws_route53_zone" "route53_zone" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  name         = "${local.domain_name}."
  private_zone = false
}

# create certificate
resource "aws_acm_certificate" "dsci_benchmark_certificate" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  domain_name       = "${local.domain_name}"
  validation_method = "DNS"

  subject_alternative_names = ["*.${local.domain_name}"]

  tags = "${var.default_tags}"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cert_validation" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  name    = "${aws_acm_certificate.dsci_benchmark_certificate.domain_validation_options.0.resource_record_name}"
  type    = "${aws_acm_certificate.dsci_benchmark_certificate.domain_validation_options.0.resource_record_type}"
  zone_id = "${data.aws_route53_zone.route53_zone.id}"
  records = ["${aws_acm_certificate.dsci_benchmark_certificate.domain_validation_options.0.resource_record_value}"]
  ttl     = 60
}

resource "aws_acm_certificate_validation" "certificate_validation" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  certificate_arn         = "${aws_acm_certificate.dsci_benchmark_certificate.arn}"
  validation_record_fqdns = ["${aws_route53_record.cert_validation.fqdn}"]
}

# MKT
resource "aws_route53_record" "route53_record_mkt" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  name    = "${local.domain_name_mkt}"
  type    = "A"
  zone_id = "${data.aws_route53_zone.route53_zone.zone_id}"

  alias {
    evaluate_target_health = false
    name                   = "${aws_api_gateway_domain_name.dsci_benchmark_mkt_custom_domain.regional_domain_name}"
    zone_id                = "${aws_api_gateway_domain_name.dsci_benchmark_mkt_custom_domain.regional_zone_id}"
  }
}

resource "aws_api_gateway_domain_name" "dsci_benchmark_mkt_custom_domain" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  domain_name              = "${local.domain_name_mkt}"
  regional_certificate_arn = "${aws_acm_certificate_validation.certificate_validation.certificate_arn}"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_base_path_mapping" "mkt-base-path" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  domain_name = "${aws_api_gateway_domain_name.dsci_benchmark_mkt_custom_domain.domain_name}"
  api_id      = "${aws_api_gateway_rest_api.dsci_benchmark_marketing_api.id}"
  stage_name  = "${aws_api_gateway_stage.dsci_benchmark_stage.stage_name}"

  base_path = "${var.api_version}"
}

#EOL

resource "aws_route53_record" "route53_record_eol" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  name    = "${local.domain_name_eol}"
  type    = "A"
  zone_id = "${data.aws_route53_zone.route53_zone.zone_id}"

  alias {
    evaluate_target_health = false
    name                   = "${aws_api_gateway_domain_name.dsci_benchmark_eol_custom_domain.regional_domain_name}"
    zone_id                = "${aws_api_gateway_domain_name.dsci_benchmark_eol_custom_domain.regional_zone_id}"
  }
}

resource "aws_api_gateway_domain_name" "dsci_benchmark_eol_custom_domain" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  domain_name = "${local.domain_name_eol}"

  regional_certificate_arn = "${aws_acm_certificate_validation.certificate_validation.certificate_arn}"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_base_path_mapping" "eol-base-path" {
  count = "${terraform.workspace == "dp" ? 1 : 0 }"

  domain_name = "${aws_api_gateway_domain_name.dsci_benchmark_eol_custom_domain.domain_name}"
  api_id      = "${aws_api_gateway_rest_api.dsci_benchmark_eol_api.id}"
  stage_name  = "${aws_api_gateway_stage.dsci_benchmark_stage.stage_name}"

  base_path = "${var.api_version}"
}
