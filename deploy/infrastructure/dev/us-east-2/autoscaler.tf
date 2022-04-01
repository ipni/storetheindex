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
  name   = "${local.environment_name}_cluster_autoscaler"
  policy = data.aws_iam_policy_document.cluster_autoscaler.json
  tags   = local.tags
}

module "cluster_autoscaler_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.17.1"

  create_role = true

  role_name    = "${local.environment_name}_cluster_autoscaler"
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.cluster_autoscaler.arn,
  ]

  oidc_fully_qualified_subjects = ["system:serviceaccount:kube-system:cluster-autoscaler"]

  tags = local.tags
}
