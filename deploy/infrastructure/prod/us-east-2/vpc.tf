module "vpc" {
  source  = "registry.terraform.io/terraform-aws-modules/vpc/aws"
  version = "3.13.0"

  name = local.environment_name

  azs = data.aws_availability_zones.available.names

  cidr            = "20.10.0.0/16"
  private_subnets = ["20.10.1.0/24", "20.10.2.0/24", "20.10.3.0/24"]
  public_subnets  = ["20.10.11.0/24", "20.10.12.0/24", "20.10.13.0/24"]

  enable_nat_gateway   = true
  enable_dns_hostnames = true
  enable_dns_support   = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}
