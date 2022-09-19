module "ecr_ue2" {
  source = "../modules/ecr"

  repositories = [
    "storetheindex/storetheindex",
    "index-observer/index-observer",
    "autoretrieve/autoretrieve",
    "index-provider/index-provider",
    "indexstar/indexstar",
    "ipfs-shipyard/ipfs-index-provider",
  ]
  tags = local.tags
}
