locals {

  environment_name = "dev"

  tags = {
    "Environment" = local.environment_name
    "ManagedBy"   = "terraform"
    Owner         = "storetheindex"
    Team          = "bedrock"
    Organization  = "EngRes"
  }
}
