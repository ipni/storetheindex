resource "aws_route53_zone" "dev_external" {
  name = "dev.cid.contact"
  tags = local.tags
}
