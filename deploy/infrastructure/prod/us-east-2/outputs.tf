output "prod_cid_contact_nameservers" {
  value = aws_route53_zone.prod_external.name_servers
}
