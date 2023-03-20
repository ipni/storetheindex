locals {
  initial_private_subnet_ids = [
    data.aws_subnet.ue2a1.id, data.aws_subnet.ue2b1.id, data.aws_subnet.ue2c1.id
  ]
  secondary_private_subnet_ids = [
    data.aws_subnet.ue2a2.id, data.aws_subnet.ue2b2.id, data.aws_subnet.ue2c2.id
  ]
}

module "vpc" {
  source  = "registry.terraform.io/terraform-aws-modules/vpc/aws"
  version = "3.13.0"

  name = local.environment_name

  azs                    = data.aws_availability_zones.available.names
  one_nat_gateway_per_az = true

  cidr = "20.10.0.0/16"
  private_subnets = [
    "20.10.1.0/24", "20.10.2.0/24", "20.10.3.0/24",
    "20.10.4.0/24", "20.10.5.0/24", "20.10.6.0/24",
    "20.10.7.0/24", "20.10.8.0/24", "20.10.9.0/24"
  ]
  public_subnets = ["20.10.11.0/24", "20.10.12.0/24", "20.10.13.0/24"]

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

data "aws_subnet" "ue2a1" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2a"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.1.0/24"]
  }
}

data "aws_subnet" "ue2b1" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2b"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.2.0/24"]
  }
}

data "aws_subnet" "ue2c1" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2c"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.3.0/24"]
  }
}

data "aws_subnet" "ue2a2" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2a"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.4.0/24"]
  }
}

data "aws_subnet" "ue2a3" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2a"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.7.0/24"]
  }
}

data "aws_subnet" "ue2b2" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2b"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.5.0/24"]
  }
}

data "aws_subnet" "ue2b3" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2b"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.8.0/24"]
  }
}

data "aws_subnet" "ue2c2" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2c"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.6.0/24"]
  }
}

data "aws_subnet" "ue2c3" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2c"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
  filter {
    name   = "cidr-block"
    values = ["20.10.9.0/24"]
  }
}
