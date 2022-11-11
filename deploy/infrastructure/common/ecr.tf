module "ecr_ue2" {
  source = "../modules/ecr"

  repositories = [
    "storetheindex/storetheindex",
    "index-observer/index-observer",
    "autoretrieve/autoretrieve",
    "index-provider/index-provider",
    "indexstar/indexstar",
    "ipni/heyfil",
  ]
  tags = local.tags
}
