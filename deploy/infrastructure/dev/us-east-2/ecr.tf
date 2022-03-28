module "ecr" {
  source = "../../modules/ecr"

  repositories = [
    "storetheindex/storetheindex",
  ]

  tags = local.tags
}
