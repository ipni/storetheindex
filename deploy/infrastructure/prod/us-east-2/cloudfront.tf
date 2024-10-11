locals {
  indexstar_origin_id     = "${local.environment_name}_${local.region}_indexstar"
  indexstar_berg_origin_id     = "${local.environment_name}_${local.region}_indexstar_berg"
  indexstar_sf_origin_id     = "${local.environment_name}_${local.region}_indexstar_sf"
  indexstar_primary = local.indexstar_sf_origin_id
  http_announce_origin_id = "${local.environment_name}_${local.region}_assigner"
  cdn_subdomain           = "cdn"
  cf_log_bucket           = "${local.environment_name}-${local.region}-cf-log"
}

resource "aws_s3_bucket" "cf_logs" {
  bucket = local.cf_log_bucket
}

resource "aws_s3_bucket_acl" "cf_logs_acl" {
  bucket = aws_s3_bucket.cf_logs.id
  acl    = "private"
}

resource "aws_cloudfront_distribution" "cdn" {
  enabled = true

  aliases = [
    "${local.cdn_subdomain}.${aws_route53_zone.prod_external.name}",
    "infra.cid.contact",
    "cid.contact",
  ]
  price_class = "PriceClass_All"

  # Disable Event logs on CloudFront. 
  # The commented out HCL below is left to re-enable logs as needed. The S3 bucket with data is left
  # to store the logs in the time-being as it is being used for diagnostics and cross team collaboration.
  #
  # TODO: Remove logging_config entirely and delete the associated S3 once no longer needed.
  #
  #  logging_config {
  #    include_cookies = false
  #    bucket          = aws_s3_bucket.cf_logs.bucket_domain_name
  #    prefix          = "${local.environment_name}_${local.region}"
  #  }

  # storetheindex/indexstar ingress.
  origin {
    domain_name = "indexstar.${aws_route53_zone.prod_external.name}"
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

  # The node named `assigner` in prod environment uses an identity that is whitelisted by Lotus 
  # bootstrap nodes in order to relay gossipsub. That node is also configured to re-propagate 
  # HTTP announces over gossipsub.
  # Therefore, all HTTP announce requests are routed to it. 
  # 
  # See: storetheindex/assigner ingress object.
  origin {
    domain_name = "assigner.${aws_route53_zone.prod_external.name}"
    origin_id   = local.http_announce_origin_id
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

  // A local load balancer which hooked up to berg.cid.contact under the hood.
  origin {
    domain_name = "indexstar-berg.${aws_route53_zone.prod_external.name}"
    origin_id   = local.indexstar_berg_origin_id
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

  // A local load balancer which hooked up to sf.cid.contact under the hood.
  origin {
    domain_name = "indexstar-sf.${aws_route53_zone.prod_external.name}"
    origin_id   = local.indexstar_sf_origin_id
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
    target_origin_id = local.indexstar_primary

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

  ordered_cache_behavior {
    path_pattern     = "multihash/*"
    # CloudFront does not support configuring allowed methods selectively.
    # Hence the complete method list.
    allowed_methods  = ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "PATCH", "POST"]
    cached_methods   = ["GET", "HEAD", "OPTIONS"]
    target_origin_id = local.indexstar_primary
    cache_policy_id  = aws_cloudfront_cache_policy.lookup.id

    compress               = true
    viewer_protocol_policy = "redirect-to-https"
  }

  ordered_cache_behavior {
    path_pattern     = "cid/*"
    allowed_methods  = ["GET", "HEAD", "OPTIONS"]
    cached_methods   = ["GET", "HEAD", "OPTIONS"]
    target_origin_id = local.indexstar_primary
    cache_policy_id  = aws_cloudfront_cache_policy.lookup.id

    compress               = true
    viewer_protocol_policy = "redirect-to-https"
  }

  ordered_cache_behavior {
    path_pattern     = "providers"
    allowed_methods  = ["GET", "HEAD", "OPTIONS"]
    cached_methods   = ["GET", "HEAD", "OPTIONS"]
    target_origin_id = local.indexstar_primary
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

  ordered_cache_behavior {
    path_pattern     = "ingest/*"
    # CloudFront does not support configuring allowed methods selectively.
    # Hence the complete method list.
    allowed_methods  = ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "PATCH", "POST"]
    cached_methods   = ["GET", "HEAD", "OPTIONS"]
    target_origin_id = local.indexstar_primary
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

resource "aws_cloudfront_cache_policy" "lookup" {
  name = "lookup"

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

provider "aws" {
  alias  = "use1"
  region = "us-east-1"
}

module "cdn_cert" {
  source  = "registry.terraform.io/terraform-aws-modules/acm/aws"
  version = "4.3.2"

  #  Certificate must be in us-east-1 as dictated by CloudFront
  providers = {
    aws = aws.use1
  }

  domain_name               = aws_route53_zone.prod_external.name
  zone_id                   = aws_route53_zone.prod_external.zone_id
  subject_alternative_names = ["*.${aws_route53_zone.prod_external.name}"]

  tags = local.tags
}

module "records" {
  source  = "registry.terraform.io/terraform-aws-modules/route53/aws//modules/records"
  version = "2.10.2"

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
  version = "4.3.2"

  #  Certificate must be in us-east-1 as dictated by CloudFront
  providers = {
    aws = aws.use1
  }

  domain_name = "cid.contact"

  # Validation is done manually by creating CNAME records on cloudflare, since nameserver for this
  # domain is managed externally from AWS via cloudflare nameservers.
  validate_certificate = false

  subject_alternative_names = [
    "*.prod.cid.contact",
    "*.infra.cid.contact",
    "*.cid.contact",
  ]

  tags = local.tags
}
