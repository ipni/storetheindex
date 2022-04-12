provider "aws" {
  region              = "us-east-1"
  alias               = "use1"
  allowed_account_ids = [local.account_id]
  default_tags {
    tags = local.tags
  }
}
