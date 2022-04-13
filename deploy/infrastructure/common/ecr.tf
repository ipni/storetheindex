module "ecr_ue2" {
  source = "../modules/ecr"

  repositories = [
    "storetheindex/storetheindex",
  ]
  tags = local.tags
}
