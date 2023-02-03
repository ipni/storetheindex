module "eks" {
  source  = "registry.terraform.io/terraform-aws-modules/eks/aws"
  version = "18.20.2"

  cluster_name    = local.environment_name
  cluster_version = "1.22"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = local.initial_private_subnet_ids

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
    # Node group used by double hashing indexer nodes
    dev-ue2a-r6a-xl = {
      min_size       = 0
      max_size       = 3
      desired_size   = 1
      instance_types = ["r6a.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a2.id]
    }
    # Node group used by dhstore nodes
    dev-ue2c-r6a-xl = {
      min_size       = 0
      max_size       = 3
      desired_size   = 1
      instance_types = ["r6a.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c2.id]
    }
    # TODO: break into node groups per subnet for less cost
    dev-ue2-m4-xl-2 = {
      min_size       = 3
      max_size       = 7
      desired_size   = 3
      instance_types = ["m4.xlarge"]
      subnet_ids     = module.vpc.private_subnets
    }
    # Node group primarily used by autoretrieve with PVC in us-east2a availability zone.
    dev-ue2a-r5a-2xl = {
      min_size       = 1
      max_size       = 7
      desired_size   = 1
      instance_types = ["r5a.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a1.id]
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
    dev-ue2b-r5b-4xl = {
      min_size       = 0
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5b.4xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b1.id]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b-4xl"
          effect = "NO_SCHEDULE"
        }
      }
    }
    dev-ue2c-r5b-4xl = {
      min_size       = 0
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5b.4xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c1.id]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b-4xl"
          effect = "NO_SCHEDULE"
        }
      }
    }

    dev-ue2b-r5n-2xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5n.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b2.id]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5n-2xl"
          effect = "NO_SCHEDULE"
        }
      }
    }
    dev-ue2c-r5n-2xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5n.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c2.id]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5n-2xl"
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
