output "kms_sti_ailas_arn" {
  value = aws_kms_alias.kms_sti.arn
}

output "kms_cluster_ailas_arn" {
  value = aws_kms_alias.kms_cluster.arn
}

output "kustomize_controller_role_arn" {
  value = module.kustomize_controller_role.iam_role_arn
}

output "external_dns_role_arn" {
  value = module.external_dns_role.iam_role_arn
}

output "cert_manager_role_arn" {
  value = module.cert_manager_role.iam_role_arn
}

output "cluster_autoscaler_role_arn" {
  value = module.cluster_autoscaler_role.iam_role_arn
}

output "ebs_csi_controller_role_arn" {
  value = module.ebs_csi_controller_role.iam_role_arn
}

output "monitoring_role_arn" {
  value = module.monitoring_role.iam_role_arn
}

output "prod_cid_contact_nameservers" {
  value = aws_route53_zone.prod_external.name_servers
}

output "berg_user_access_key_id" {
  value = module.berg_user.iam_access_key_id
}

output "berg_user_access_key_secret_encrypted" {
  value = module.berg_user.iam_access_key_encrypted_secret
}
