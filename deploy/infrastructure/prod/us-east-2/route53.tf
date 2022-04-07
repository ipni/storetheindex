resource "aws_route53_zone" "prod_external" {
  name = "${local.environment_name}.cid.contact"
}
