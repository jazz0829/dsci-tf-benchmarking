data "terraform_remote_state" "base_config" {
  backend = "s3"

  config {
    region                  = "eu-west-1"
    bucket                  = "excp-glbl-tfm-remote-state"
    key                     = "env:/${terraform.workspace}/dsci-tf-ml-orchestration-lambdas.tfstate"
    encrypt                 = "true"
    shared_credentials_file = "~/.aws/credentials"
    profile                 = "ci-glbl-auto"
  }
}
