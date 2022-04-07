locals {

  environment_name = "prod"
  region           = data.aws_region.current.name

  iam_path = "/${local.environment_name}/${local.region}/"

  tags = {
    "Environment" = local.environment_name
    "ManagedBy"   = "terraform"
    Owner         = "storetheindex"
    Team          = "bedrock"
    Organization  = "EngRes"
  }
}

data "aws_region" "current" {}
