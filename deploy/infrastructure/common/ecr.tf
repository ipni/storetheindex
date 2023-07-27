module "ecr_ue2" {
  source = "../modules/ecr"

  repositories = [
    "storetheindex/storetheindex",
    "index-observer/index-observer",
    "index-provider/index-provider",
    "indexstar/indexstar",
    "ipni/heyfil",
    "ipni/dhstore",
    "ipni/caskadht",
    "ipni/dhfind",
    "ipni/lookout",
    "ipni/cassette",
    "ipni/telemetry",
  ]
  tags = local.tags
}
