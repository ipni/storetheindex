terraform {
  backend "s3" {
    bucket = "sti-dev-terraform-state"
    key    = "sti/common/terraform.tfstate"
    region = "us-east-2"
  }
}
