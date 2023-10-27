module "eks" {
  source  = "registry.terraform.io/terraform-aws-modules/eks/aws"
  version = "18.20.2"

  cluster_name    = local.environment_name
  cluster_version = "1.23"

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
    # Smaller memory optimised node groups primarily used to run indexer and dhstore nodes with
    # lighter duty work.
    dev-ue2a-r6a-xl = {
      min_size       = 0
      max_size       = 5
      desired_size   = 1
      instance_types = ["r6a.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a1.id, data.aws_subnet.ue2a2.id, data.aws_subnet.ue2a3.id]
    }
    dev-ue2b-r6a-xl = {
      min_size       = 0
      max_size       = 5
      desired_size   = 1
      instance_types = ["r6a.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b1.id, data.aws_subnet.ue2b2.id, data.aws_subnet.ue2b3.id]
    }
    dev-ue2c-r6a-xl = {
      min_size       = 0
      max_size       = 5
      desired_size   = 1
      instance_types = ["r6a.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c1.id, data.aws_subnet.ue2c2.id, data.aws_subnet.ue2c3.id]
    }

    # General purpose node groups, one per subnet.
    # They are split per subnet so that:
    #  - we can scale down to zero instances in any subnet we are not running any pods.
    #  - we work around the inability to control subnets in which ASG spins up instances.
    dev-ue2a-m4-xl = {
      min_size       = 0
      max_size       = 7
      desired_size   = 1
      instance_types = ["m4.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a1.id, data.aws_subnet.ue2a2.id]
    }
    dev-ue2b-m4-xl = {
      min_size       = 0
      max_size       = 7
      desired_size   = 1
      instance_types = ["m4.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b1.id, data.aws_subnet.ue2b2.id]
    }
    dev-ue2c-m4-xl = {
      min_size       = 0
      max_size       = 7
      desired_size   = 1
      instance_types = ["m4.xlarge"]
      subnet_ids     = [data.aws_subnet.ue2c1.id, data.aws_subnet.ue2c2.id]
    }

    # Node group primarily used by tornado with PVC in us-east2a availability zone.
    dev-ue2a-r5a-2xl = {
      min_size       = 1
      max_size       = 7
      desired_size   = 1
      instance_types = ["r5a.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a1.id, data.aws_subnet.ue2a2.id, data.aws_subnet.ue2a3.id]
    }
    # Used by foundation db
    dev-ue2-r5a-2xl = {
      min_size       = 0
      max_size       = 15
      desired_size   = 1
      instance_types = ["r5a.2xlarge"]
      subnet_ids = [
        data.aws_subnet.ue2a1.id, data.aws_subnet.ue2a2.id, data.aws_subnet.ue2a3.id,
        data.aws_subnet.ue2b1.id, data.aws_subnet.ue2b2.id, data.aws_subnet.ue2b3.id,
        data.aws_subnet.ue2c1.id, data.aws_subnet.ue2c2.id, data.aws_subnet.ue2c3.id,
      ]
    }

    # Memory optimised node groups primarily used to run indexer nodes.
    dev-ue2a-r5n-2xl = {
      min_size       = 1
      max_size       = 5
      desired_size   = 1
      instance_types = ["r5n.2xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a2.id]
      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "r5n-2xl"
          effect = "NO_SCHEDULE"
        }
      }
    }
    dev-ue2b-r5n-2xl = {
      min_size       = 1
      max_size       = 5
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
      max_size       = 5
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
