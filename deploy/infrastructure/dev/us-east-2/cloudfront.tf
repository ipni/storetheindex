locals {
  indexstar_origin_id     = "${local.environment_name}_${local.region}_indexstar"
  http_announce_origin_id = "${local.environment_name}_${local.region}_http_announce"
  cdn_subdomain           = "cdn"
}

resource "aws_cloudfront_distribution" "cdn" {
  enabled = true

  aliases = [
    aws_route53_zone.dev_external.name,
    "${local.cdn_subdomain}.${aws_route53_zone.dev_external.name}",
  ]
  price_class = "PriceClass_All"

  # The node named `assigner` in dev environment uses an identity that is whitelisted by Lotus 
  # bootstrap nodes in order to relay gossipsub. That node is also configured to re-propagate 
  # HTTP announces over gossipsub.
  # Therefore, all HTTP announce requests are routed to it. 
  # 
  # See: storetheindex/assigner-indexer ingress object.
  origin {
    domain_name = "assigner.${aws_route53_zone.dev_external.name}"
    origin_id   = local.http_announce_origin_id
    custom_origin_config {
      http_port              = 80
      https_port             = 443
      origin_protocol_policy = "https-only"
      origin_ssl_protocols   = ["SSLv3", "TLSv1", "TLSv1.1", "TLSv1.2"]
    }
    origin_shield {
      enabled              = true
      origin_shield_region = data.aws_region.current.name
    }
  }

  # storetheindex/indexstar ingress.
  origin {
    domain_name = "indexstar.${aws_route53_zone.dev_external.name}"
    origin_id   = local.indexstar_origin_id
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
    # We need to allow GET and PUT. CloudFront does not support configuring allowed methods selectively.
    # Hence the complete method list.
    allowed_methods  = ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "PATCH", "POST"]
    cached_methods   = ["GET", "HEAD", "OPTIONS"]
    target_origin_id = local.indexstar_origin_id
    cache_policy_id  = aws_cloudfront_cache_policy.lookup.id

    compress               = true
    viewer_protocol_policy = "redirect-to-https"
  }

  ordered_cache_behavior {
    path_pattern           = "reframe"
    # CloudFront does not support configuring allowed methods selectively.
    # Hence the complete method list.
    allowed_methods        = ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "PATCH", "POST"]
    cached_methods         = ["GET", "HEAD", "OPTIONS"]
    target_origin_id       = local.indexstar_origin_id
    cache_policy_id        = aws_cloudfront_cache_policy.reframe.id
    viewer_protocol_policy = "redirect-to-https"
  }

  ordered_cache_behavior {
    path_pattern     = "ingest/*"
    # CloudFront does not support configuring allowed methods selectively.
    # Hence the complete method list.
    allowed_methods  = ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "PATCH", "POST"]
    cached_methods   = ["GET", "HEAD", "OPTIONS"]
    target_origin_id = local.http_announce_origin_id
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
    acm_certificate_arn = module.cdn_cert.acm_certificate_arn
    ssl_support_method  = "sni-only"

  }

  tags = local.tags
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

  domain_name               = aws_route53_zone.dev_external.name
  zone_id                   = aws_route53_zone.dev_external.zone_id
  subject_alternative_names = ["*.${aws_route53_zone.dev_external.name}"]

  tags = local.tags
}

module "records" {
  source  = "registry.terraform.io/terraform-aws-modules/route53/aws//modules/records"
  version = "2.6.0"

  zone_id = aws_route53_zone.dev_external.zone_id

  records = [
    {
      # Point `dev.cid.contact` to cloudfront distribution.
      name  = ""
      type  = "A"
      alias = {
        name    = aws_cloudfront_distribution.cdn.domain_name
        zone_id = aws_cloudfront_distribution.cdn.hosted_zone_id
      }
    },
    {
      # Point `cdn.dev.cid.contact` to cloudfront distribution for backward compatibility.
      name  = local.cdn_subdomain
      type  = "A"
      alias = {
        name    = aws_cloudfront_distribution.cdn.domain_name
        zone_id = aws_cloudfront_distribution.cdn.hosted_zone_id
      }
    },
  ]
}

resource "aws_cloudfront_cache_policy" "reframe" {
  name = "${local.environment_name}_reframe"

  # We have to set non-zero TTL values because otherwise CloudFront won't let 
  # the query strings settings to be configured.
  min_ttl     = 0
  default_ttl = 3600
  max_ttl     = 86400

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "all"
    }

    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}

resource "aws_cloudfront_cache_policy" "lookup" {
  name = "${local.environment_name}_lookup"

  # We have to set non-zero TTL values because otherwise CloudFront won't let 
  # the query strings settings to be configured.
  min_ttl     = 0
  default_ttl = 3600
  max_ttl     = 86400

  parameters_in_cache_key_and_forwarded_to_origin {
    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "whitelist"
      headers {
        items = ["Accept"]
      }
    }
    query_strings_config {
      query_string_behavior = "whitelist"
      query_strings {
        items = ["cascade"]
      }
    }

    enable_accept_encoding_brotli = true
    enable_accept_encoding_gzip   = true
  }
}
