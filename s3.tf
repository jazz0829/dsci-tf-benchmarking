# force rerun of the circle ci build
resource "aws_s3_bucket" "dsci_s3_bucket_marketing" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  acl    = "private"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "benchmark_raw" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/1_benchmark_raw.py"
  source = "${path.module}/scripts/Marketing/1_benchmark_raw.py"
  etag   = "${md5(file("${path.module}/scripts/Marketing/1_benchmark_raw.py"))}"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "benchmark_division" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/2_benchmark_division_level.py"
  source = "${path.module}/scripts/Marketing/2_benchmark_division_level.py"
  etag   = "${md5(file("${path.module}/scripts/Marketing/2_benchmark_division_level.py"))}"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "benchmark_aggregation" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/3_benchmark_aggregation.py"
  source = "${path.module}/scripts/Marketing/3_benchmark_aggregation.py"
  etag   = "${md5(file("${path.module}/scripts/Marketing/3_benchmark_aggregation.py"))}"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "benchmark_elements" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/eol2_benchmark_elements.py"
  source = "${path.module}/scripts/EOL/eol2_benchmark_elements.py"
  etag   = "${md5(file("${path.module}/scripts/EOL/eol2_benchmark_elements.py"))}"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "benchmark_eol_division" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/eol3_benchmark_division_level.py"
  source = "${path.module}/scripts/EOL/eol3_benchmark_division_level.py"
  etag   = "${md5(file("${path.module}/scripts/EOL/eol3_benchmark_division_level.py"))}"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "benchmark_eol_aggregation" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/eol4_benchmark_aggregation.py"
  source = "${path.module}/scripts/EOL/eol4_benchmark_aggregation.py"
  etag   = "${md5(file("${path.module}/scripts/EOL/eol4_benchmark_aggregation.py"))}"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "rgs1-1-csv" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/RGS1point1_lev3.csv"
  source = "${path.module}/scripts/RGS1point1_lev3.csv"
  etag   = "${md5(file("${path.module}/scripts/RGS1point1_lev3.csv"))}"

  tags = "${var.default_tags}"
}

resource "aws_s3_bucket_object" "install_dependencies" {
  bucket = "${local.dsci_benchmark_bucket_name}"
  key    = "scripts/EMR/install_dependencies.sh"
  source = "${path.module}/scripts/install_dependencies.sh"
  etag   = "${md5(file("${path.module}/scripts/install_dependencies.sh"))}"

  tags = "${var.default_tags}"
}
