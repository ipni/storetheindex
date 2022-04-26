locals {
  cdn_origin_id = "${local.environment_name}_${local.region}_sti_cdn"
  cdn_subdomain = "cdn"
}

resource "aws_cloudfront_distribution" "cdn" {
  enabled = true

  aliases = [
    "${local.cdn_subdomain}.${aws_route53_zone.prod_external.name}",
    "infra.cid.contact",
    "cid.contact",
  ]
  price_class = "PriceClass_All"

  origin {
    domain_name = aws_route53_zone.prod_external.name
    origin_id   = local.cdn_origin_id
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["SSLv3", "TLSv1", "TLSv1.1", "TLSv1.2"]
    }
    origin_shield {
      enabled              = true
      origin_shield_region = local.region
    }
  }

  custom_error_response {
    error_code            = 404
    error_caching_min_ttl = 300
  }

  default_cache_behavior {
    allowed_methods  = ["GET", "HEAD", "OPTIONS"]
    cached_methods   = ["GET", "HEAD"]
    target_origin_id = local.cdn_origin_id

    forwarded_values {
      query_string = false

      cookies {
        forward = "none"
      }
    }
    compress               = true
    viewer_protocol_policy = "redirect-to-https"
    min_ttl                = 0
    default_ttl            = 3600
    max_ttl                = 86400
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }
  viewer_certificate {
    acm_certificate_arn = module.cid_contact_cert.acm_certificate_arn
    ssl_support_method  = "sni-only"
  }
}

provider "aws" {
  alias  = "use1"
  region = "us-east-1"
}

module "cdn_cert" {
  source  = "registry.terraform.io/terraform-aws-modules/acm/aws"
  version = "3.4.0"

  #  Certificate must be in us-east-1 as dictated by CloudFront
  providers = {
    aws = aws.use1
  }

  domain_name               = aws_route53_zone.prod_external.name
  zone_id                   = aws_route53_zone.prod_external.zone_id
  subject_alternative_names = ["*.${aws_route53_zone.prod_external.name}"]
}

module "records" {
  source  = "registry.terraform.io/terraform-aws-modules/route53/aws//modules/records"
  version = "2.6.0"

  zone_id = aws_route53_zone.prod_external.zone_id

  records = [
    {
      name  = local.cdn_subdomain
      type  = "A"
      alias = {
        name    = aws_cloudfront_distribution.cdn.domain_name
        zone_id = aws_cloudfront_distribution.cdn.hosted_zone_id
      }
    },
  ]
}

module "cid_contact_cert" {
  source  = "registry.terraform.io/terraform-aws-modules/acm/aws"
  version = "3.4.0"

  #  Certificate must be in us-east-1 as dictated by CloudFront
  providers = {
    aws = aws.use1
  }

  domain_name = "cid.contact"

  # Validation is done manually by creating CNAME records on cloudflare, since nameserver for this
  # domain is managed externally from AWS via cloudflare nameservers.
  validate_certificate = false

  subject_alternative_names = [
    "cid.contact",
    "*.cid.contact",
    "*.prod.cid.contact",
    "*.infra.cid.contact",
  ]
}
