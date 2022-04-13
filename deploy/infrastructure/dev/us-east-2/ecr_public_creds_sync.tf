data "aws_iam_policy_document" "ecr_public_creds_sync" {
  statement {
    effect = "Allow"

    actions = [
      "ecr-public:GetAuthorizationToken",
    ]

    resources = [aws_kms_key.kms_sti.arn]
  }
}

resource "aws_iam_policy" "ecr_public_creds_sync" {
  name   = "${local.environment_name}_ecr_public_creds_sync"
  policy = data.aws_iam_policy_document.ecr_public_creds_sync.json
  tags   = local.tags
}

module "ecr_public_creds_sync_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.17.1"

  create_role = true

  role_name    = "${local.environment_name}_ecr_public_creds_sync"
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.ecr_public_creds_sync.arn,
  ]

  oidc_fully_qualified_subjects = [
    "system:serviceaccount:storetheindex:ecr-public-credentials-sync"
  ]

  tags = local.tags
}
