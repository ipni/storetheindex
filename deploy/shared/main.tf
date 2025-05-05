terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.86.0"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5"
    }
  }
  backend "s3" {
    bucket = "storacha-terraform-state"
   
    region = "us-west-2"
    encrypt = true
  }
}

provider "aws" {
  allowed_account_ids = [var.allowed_account_id]
  region = var.region
  default_tags {
    tags = {
      "Environment" = terraform.workspace
      "ManagedBy"   = "OpenTofu"
      Owner         = "storacha"
      Team          = "Storacha Engineering"
      Organization  = "Storacha"
      Project       = "${var.app}"
    }
  }
}

module "shared" {
  source = "github.com/storacha/storoku//shared?ref=v0.2.13"
  create_db = false
  caches = [  ]
  app = var.app
  zone_id = var.cloudflare_zone_id
  domain_base = var.domain_base
  setup_cloudflare = true
}