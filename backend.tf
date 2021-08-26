terraform {
  required_version = "~>0.11.11"

  backend "s3" {
    bucket                  = "excp-glbl-tfm-remote-state"
    region                  = "eu-west-1"
    key                     = "dsci-tf-benchmarking.tfstate"
    encrypt                 = "true"
    shared_credentials_file = "~/.aws/credentials"
    profile                 = "ci-glbl-auto"
  }
}
