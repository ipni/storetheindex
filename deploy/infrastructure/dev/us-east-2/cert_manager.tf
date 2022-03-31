data "aws_iam_policy_document" "cert_manager" {
  statement {
    effect    = "Allow"
    actions   = ["route53:GetChange"]
    resources = ["arn:aws:route53:::change/*"]
  }
  statement {
    effect = "Allow"
    actions = [
      "route53:ChangeResourceRecordSets",
      "route53:ListResourceRecordSets"
    ]
    resources = ["arn:aws:route53:::hostedzone/*"]
  }
  statement {
    effect    = "Allow"
    actions   = ["route53:ListHostedZonesByName"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "cert_manager" {
  name   = "${local.environment_name}_cert_manager"
  policy = data.aws_iam_policy_document.cert_manager.json
  tags   = local.tags
}

module "cert_manager_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.17.1"

  create_role = true

  role_name    = "${local.environment_name}_cert_manager"
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.cert_manager.arn,
  ]

  oidc_fully_qualified_subjects = [
    "system:serviceaccount:cert-manager:cert-manager",
    "system:serviceaccount:cert-manager:cert-manager-webhook",
  ]

  tags = local.tags
}
