module "ecr_ue2" {
  source = "../modules/ecr"

  repositories = [
    "storetheindex/storetheindex",
    "index-observer/index-observer",
    "autoretrieve/autoretrieve",
    "index-provider/index-provider",
    "indexstar/indexstar",
    "ipni/heyfil",
    "ipni/dhstore",
    "ipni/caskadht",
    "ipni/dhfind",
    "ipni/lookout",
    "ipni/cassette",
  ]
  tags = local.tags
}
