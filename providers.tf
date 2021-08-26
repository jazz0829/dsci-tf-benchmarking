provider "aws" {
  version                 = "~>2.2"
  region                  = "${var.region}"
  shared_credentials_file = "~/.aws/credentials"
  profile                 = "default"
  allowed_account_ids     = ["${var.accountid}"]
}

provider "external" {}
provider "archive" {}
provider "null" {}
provider "template" {}
