data "aws_iam_policy_document" "external_dns" {
  statement {
    sid    = "ChangeAndListRecordsOnDevZone"
    effect = "Allow"
    actions = [
      "route53:ChangeResourceRecordSets",
    ]
    resources = ["arn:aws:route53:::hostedzone/*"]
  }
  statement {
    sid    = "ListZonesAndRecords"
    effect = "Allow"
    actions = [
      "route53:ListHostedZones",
      "route53:ListResourceRecordSets",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "external_dns" {
  name   = "${local.environment_name}_external_dns"
  policy = data.aws_iam_policy_document.external_dns.json
  path   = local.iam_path
}

module "external_dns_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "5.20.0"

  create_role = true

  role_name    = "external_dns"
  role_path    = local.iam_path
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.external_dns.arn,
  ]

  oidc_fully_qualified_subjects = ["system:serviceaccount:external-dns:external-dns"]
}
