terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.86.0"
    }
    archive = {
      source = "hashicorp/archive"
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

# CloudFront is a global service. Certs must be created in us-east-1, where the core ACM infra lives
provider "aws" {
  region = "us-east-1"
  alias = "acm"
}



module "app" {
  source = "github.com/storacha/storoku//app?ref=v0.2.13"
  private_key = var.private_key
  principal_mapping = var.principal_mapping
  did = var.did
  app = var.app
  appState = var.app
  environment = terraform.workspace
  # if there are any env vars you want available only to your container
  # in the vpc as opposed to set in the dockerfile, enter them here
  # NOTE: do not put sensitive data in env-vars. use secrets
  deployment_env_vars = []
  image_tag = var.image_tag
  create_db = false
  # enter secret values your app will use here -- these will be available
  # as env vars in the container at runtime
  secrets = {   }
  # enter any sqs queues you want to create here
  queues = []
  caches = [  ]
  topics = [  ]
  tables = []
  buckets = []
  providers = {
    aws = aws
    aws.acm = aws.acm
  }
  env_files = var.env_files
  domain_base = var.domain_base
}