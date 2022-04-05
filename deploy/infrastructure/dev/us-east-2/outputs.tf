output "kms_sti_flux_arn" {
  value = aws_kms_alias.kms_sti.arn
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

output "monitoring_role_arn" {
  value = module.monitoring_role.iam_role_arn
}

output "dev_cid_contact_nameservers" {
  value = aws_route53_zone.dev_external.name_servers
}

output "dev_cid_contact_zone_id" {
  value = aws_route53_zone.dev_external.zone_id
}
