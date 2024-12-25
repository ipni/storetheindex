module "ecr_ue2" {
  source = "../modules/ecr"

  repositories = []
  tags = local.tags
}
