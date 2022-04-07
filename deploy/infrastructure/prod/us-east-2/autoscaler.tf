data "aws_iam_policy_document" "cluster_autoscaler" {
  statement {
    effect = "Allow"

    actions = [
      "autoscaling:DescribeAutoScalingGroups",
      "autoscaling:DescribeAutoScalingInstances",
      "autoscaling:DescribeLaunchConfigurations",
      "autoscaling:DescribeTags",
      "autoscaling:SetDesiredCapacity",
      "autoscaling:TerminateInstanceInAutoScalingGroup",
      "ec2:DescribeLaunchTemplateVersions"
    ]

    resources = ["*"]
  }
}

resource "aws_iam_policy" "cluster_autoscaler" {
  name   = "cluster_autoscaler"
  policy = data.aws_iam_policy_document.cluster_autoscaler.json
  path   = local.iam_path
}

module "cluster_autoscaler_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.17.1"

  create_role = true

  role_name    = "cluster_autoscaler"
  role_path    = local.iam_path
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.cluster_autoscaler.arn,
  ]

  oidc_fully_qualified_subjects = ["system:serviceaccount:kube-system:cluster-autoscaler"]
}
