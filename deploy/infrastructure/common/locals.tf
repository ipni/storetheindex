locals {

  environment_name = "common"
  account_id       = "407967248065"

  iam_path = "/${local.environment_name}/"

  tags = {
    "Environment" = local.environment_name
    "ManagedBy"   = "terraform"
    Owner         = "storetheindex"
    Team          = "bedrock"
    Organization  = "EngRes"
  }
}

data "aws_region" "current" {}
