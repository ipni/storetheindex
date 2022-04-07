provider "aws" {
  region              = "us-east-2"
  allowed_account_ids = ["407967248065"]
  default_tags {
    tags = local.tags
  }
}
