output "monitoring_role_arn" {
  value = module.monitoring_role.iam_role_arn
}

output "prod_cid_contact_nameservers" {
  value = aws_route53_zone.prod_external.name_servers
}
