module "eks" {
  source  = "registry.terraform.io/terraform-aws-modules/eks/aws"
  version = "18.20.2"

  cluster_name    = local.environment_name
  cluster_version = "1.22"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_group_defaults = {
    # Enforce explicit naming of nodegrups and roles to avoid generated names
    use_name_prefix                 = false
    iam_role_use_name_prefix        = false
    security_group_use_name_prefix  = false
    launch_template_use_name_prefix = false

    # Use EKS's own default launch template
    create_launch_template = false
    launch_template_name   = ""
  }

  eks_managed_node_groups = {
    dev-ue2-m4-xl = {
      min_size       = 3
      max_size       = 7
      desired_size   = 3
      instance_types = ["m4.xlarge"]
    }
    dev-ue2-r5a-2xl = {
      min_size       = 1
      max_size       = 7
      desired_size   = 1
      instance_types = ["r5a.2xlarge"]
    }
    dev-ue2-r5b-xl = {
      min_size       = 3
      max_size       = 3
      desired_size   = 3
      instance_types = ["r5b.xlarge"]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b"
          effect = "NO_SCHEDULE"
        }
      }
    }
    dev-ue2b-r5b-2xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5b.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b.id]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b"
          effect = "NO_SCHEDULE"
        }
      }
    }
    dev-ue2c-r5b-2xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5b.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c.id]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b"
          effect = "NO_SCHEDULE"
        }
      }
    }
  }

  tags = local.tags
}

data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_id
}

data "aws_subnet" "ue2b" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2b"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
}

data "aws_subnet" "ue2c" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2c"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
}
