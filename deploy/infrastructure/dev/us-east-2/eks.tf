module "eks" {
  source  = "registry.terraform.io/terraform-aws-modules/eks/aws"
  version = "18.15.0"

  cluster_name    = local.environment_name
  cluster_version = "1.21"

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_group_defaults = {
    instance_types = ["m4.large"]
  }

  eks_managed_node_groups = {
    default_node_group = {
      min_size     = 1
      max_size     = 7
      desired_size = 1

      create_launch_template = false
      launch_template_name   = ""
    }
  }

  tags = local.tags
}

data "aws_eks_cluster_auth" "this" {
  name = module.eks.cluster_id
}
