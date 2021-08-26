resource "aws_api_gateway_account" "general_api_gateway_settings" {
  cloudwatch_role_arn = "${aws_iam_role.api_gateway_cloudwatch_role.arn}"
}

resource "aws_iam_role" "api_gateway_cloudwatch_role" {
  name = "${local.dsci_api_gateway_push_logs_to_cloudwatch_role_name}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "apigateway.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

  tags = "${var.default_tags}"
}

resource "aws_iam_role_policy" "cloudwatch" {
  name = "${local.dsci_api_gateway_push_logs_to_cloudwatch_policy_name}"
  role = "${aws_iam_role.api_gateway_cloudwatch_role.id}"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutLogEvents",
                "logs:GetLogEvents",
                "logs:FilterLogEvents"
            ],
            "Resource": "*"
        }
    ]
}
EOF
}
