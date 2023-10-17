module "eks" {
  source  = "registry.terraform.io/terraform-aws-modules/eks/aws"
  version = "18.20.2"

  cluster_name    = local.environment_name
  cluster_version = "1.23"

  cluster_endpoint_private_access = true
  cluster_endpoint_public_access  = true

  iam_role_path = local.iam_path

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
    # Node groups for running dhstore nodes
    prod-ue2c-r5n-2xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5n.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c2.id]
      taints         = {
        dedicated = {
          key    = "dedicated"
          value  = "r5n-2xl"
          effect = "NO_SCHEDULE"
        }
      }
    }
    prod-ue2a-r5n-2xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5n.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a1.id]
      taints         = {
        dedicated = {
          key    = "dedicated"
          value  = "r5n-2xl"
          effect = "NO_SCHEDULE"
        }
      }
    }


    # Node group for running double hashed indexer nodes
    prod-ue2c-r6a-xl = {
      min_size       = 0
      max_size       = 5
      desired_size   = 1
      instance_types = ["r6a.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c2.id, data.aws_subnet.ue2c3.id]
    }

    # General purpose node-group.
    prod-ue2a-m4-xl = {
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      subnet_ids     = [data.aws_subnet.ue2a2.id, data.aws_subnet.ue2a3.id]
      instance_types = ["m4.xlarge"]
    }
    prod-ue2b-m4-xl = {
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      subnet_ids     = [data.aws_subnet.ue2b2.id, data.aws_subnet.ue2b3.id]
      instance_types = ["m4.xlarge"]
    }
    prod-ue2c-m4-xl = {
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      subnet_ids     = [data.aws_subnet.ue2c2.id, data.aws_subnet.ue2c3.id]
      instance_types = ["m4.xlarge"]
    }

    prod-ue2a-c6a-8xl-2 = {
      min_size       = 0
      max_size       = 5
      desired_size   = 0
      instance_types = ["c6a.8xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a2.id, data.aws_subnet.ue2a3.id]
    }
    prod-ue2b-c6a-8xl-2 = {
      min_size       = 0
      max_size       = 5
      desired_size   = 0
      instance_types = ["c6a.8xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b2.id, data.aws_subnet.ue2b3.id]
    }
    prod-ue2c-c6a-8xl-2 = {
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      instance_types = ["c6a.8xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c2.id, data.aws_subnet.ue2c3.id]
    }
    prod-ue2a-r6a-2xl = {
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      instance_types = ["r6a.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a1.id, data.aws_subnet.ue2a2.id, data.aws_subnet.ue2a3.id]
    }
    prod-ue2b-r6a-2xl = {
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      instance_types = ["r6a.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b1.id, data.aws_subnet.ue2b2.id, data.aws_subnet.ue2b3.id]
    }
    prod-ue2c-r6a-2xl = {
      min_size       = 0
      max_size       = 10
      desired_size   = 0
      instance_types = ["r6a.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c1.id, data.aws_subnet.ue2c2.id, data.aws_subnet.ue2c3.id]
    }
    prod-ue2-c6a-8xl = {
      min_size       = 0
      max_size       = 20
      desired_size   = 2
      instance_types = ["c6a.8xlarge"]
      subnet_ids     = [
        data.aws_subnet.ue2a1.id, data.aws_subnet.ue2a2.id, data.aws_subnet.ue2a3.id,
        data.aws_subnet.ue2b1.id, data.aws_subnet.ue2b2.id, data.aws_subnet.ue2b3.id,
        data.aws_subnet.ue2c1.id, data.aws_subnet.ue2c2.id, data.aws_subnet.ue2c3.id
      ]
    }
    prod-ue2-c6a-12xl = {
      min_size       = 0
      max_size       = 20
      desired_size   = 1
      instance_types = ["c6a.12xlarge"]
      subnet_ids     = module.vpc.private_subnets
    }
  }
}

