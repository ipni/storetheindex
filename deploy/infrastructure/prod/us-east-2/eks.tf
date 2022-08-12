module "eks" {
  source  = "registry.terraform.io/terraform-aws-modules/eks/aws"
  version = "18.20.2"

  cluster_name    = local.environment_name
  cluster_version = "1.22"

  cluster_endpoint_private_access = true
  cluster_endpoint_public_access  = true

  iam_role_path = local.iam_path

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
    # General purpose node-group.
    prod-ue2-m4-xl = {
      min_size       = 1
      max_size       = 7
      desired_size   = 1
      instance_types = ["m4.xlarge"]
    }

    # Supports high IOPS via io2 Block Express EBS volume types with specific taint.
    # See:
    #  - https://aws.amazon.com/ec2/instance-types/#Memory_Optimized
    #  - https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-volume-types.html#solid-state-drives
    prod-ue2-r5b-xl = {
      min_size       = 3
      max_size       = 7
      desired_size   = 3
      instance_types = ["r5b.xlarge"]
      taints         = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b"
          effect = "NO_SCHEDULE"
        }
      }
    }
    prod-ue2a-r5b-4xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5b.4xlarge"]
      subnet_ids     = [data.aws_subnet.ue2a.id]
      taints         = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b-4xl"
          effect = "NO_SCHEDULE"
        }
      }
    }
    prod-ue2b-r5b-4xl = {
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["r5b.4xlarge"]
      subnet_ids     = [data.aws_subnet.ue2b.id]
      taints         = {
        dedicated = {
          key    = "dedicated"
          value  = "r5b-4xl"
          effect = "NO_SCHEDULE"
        }
      }
    }
  }
}

data "aws_subnet" "ue2a" {
  vpc_id = module.vpc.vpc_id

  filter {
    name   = "availability-zone"
    values = ["us-east-2a"]
  }
  filter {
    name   = "subnet-id"
    values = module.vpc.private_subnets
  }
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

