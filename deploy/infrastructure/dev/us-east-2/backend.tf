terraform {
  backend "s3" {
    bucket = "sti-dev-terraform-state"
    key    = "sti/dev/us-east-2/terraform.tfstate"
    region = "us-east-2"
  }
}
