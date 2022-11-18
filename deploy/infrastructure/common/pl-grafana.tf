module "pl_grafana_user" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-user"
  version = "4.18.0"

  name = "pl-grafana"

  path = local.iam_path

  # Base64 encoded GPG public key used to encrypt the password and secret access key of pl-grafana user.
  # The private key is stored at `storetheidnex` 1Password vault.
  pgp_key = local.pgp_key

  password_reset_required = false
  create_iam_access_key   = true

  tags = local.tags
}

module "metrics_reader_group" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-group-with-policies"
  version = "4.18.0"

  name = "metrics_viewer"
  group_users = [
    module.pl_grafana_user.iam_user_name
  ]

  create_group                      = true
  attach_iam_self_management_policy = false

  custom_group_policy_arns = [
    "arn:aws:iam::aws:policy/AmazonPrometheusQueryAccess",
  ]
}
