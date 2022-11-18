output "github_actions_role_arn" {
  value = module.github_actions_role.iam_role_arn
}

output "pl_grafana_user_password_encrypted" {
  value = module.pl_grafana_user.keybase_password_pgp_message
}

output "pl_grafana_user_access_key_id" {
  value = module.pl_grafana_user.iam_access_key_id
}

output "pl_grafana_user_access_key_secret_encrypted" {
  value = module.pl_grafana_user.iam_access_key_encrypted_secret
}

output "ipni_gh_mgmt_ro_user_access_key_id" {
  value = module.ipni_gh_mgmt_ro.iam_access_key_id
}

output "ipni_gh_mgmt_ro_user_access_key_secret_encrypted" {
  value = module.ipni_gh_mgmt_ro.iam_access_key_encrypted_secret
}

output "ipni_gh_mgmt_rw_user_access_key_id" {
  value = module.ipni_gh_mgmt_rw.iam_access_key_id
}

output "ipni_gh_mgmt_rw_user_access_key_secret_encrypted" {
  value = module.ipni_gh_mgmt_rw.iam_access_key_encrypted_secret
}
