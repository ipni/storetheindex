resource "aws_prometheus_workspace" "monitoring" {
  alias = local.environment_name
  tags  = local.tags
}
# Placeholder for setting up remote_write and hook up to PL graphana
data "aws_iam_policy_document" "monitoring" {
  statement {
    effect = "Allow"

    actions = [
      "aps:RemoteWrite",
      "aps:QueryMetrics",
      "aps:GetSeries",
      "aps:GetLabels",
      "aps:GetMetricMetadata"
    ]

    resources = [aws_prometheus_workspace.monitoring.arn]
  }
}

resource "aws_iam_policy" "monitoring" {
  name   = "${local.environment_name}_monitoring"
  policy = data.aws_iam_policy_document.monitoring.json
  tags   = local.tags
}

module "monitoring_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.17.1"

  create_role = true

  role_name    = "${local.environment_name}_monitoring"
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.monitoring.arn,
  ]

  oidc_fully_qualified_subjects = ["system:serviceaccount:monitoring:prometheus-k8s"]

  tags = local.tags
}
