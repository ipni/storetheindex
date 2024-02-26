resource "aws_iam_openid_connect_provider" "github" {
  client_id_list = [
    "https://github.com/filecoin-project",
    "https://github.com/filecoin-shipyard",
    "sts.amazonaws.com"
  ]

  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd"
  ]
  url = "https://token.actions.githubusercontent.com"
}

data "aws_iam_policy_document" "github_actions" {
  statement {
    effect = "Allow"
    actions = [
      "ecr:GetAuthorizationToken",
      "ecr:BatchCheckLayerAvailability",
      "ecr:CompleteLayerUpload",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
      "ecr-public:BatchCheckLayerAvailability",
      "ecr-public:CompleteLayerUpload",
      "ecr-public:InitiateLayerUpload",
      "ecr-public:PutImage",
      "ecr-public:UploadLayerPart",
      "ecr-public:GetAuthorizationToken",
      "sts:GetServiceBearerToken",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "github_actions" {
  name   = "github_actions"
  policy = data.aws_iam_policy_document.github_actions.json
  path   = local.iam_path
}

module "github_actions_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.18.0"

  create_role = true

  role_name    = "github_actions"
  role_path    = local.iam_path
  provider_url = aws_iam_openid_connect_provider.github.url

  role_policy_arns = [
    aws_iam_policy.github_actions.arn,
  ]

  oidc_subjects_with_wildcards = [
    "repo:ipni/*:*"
  ]
}
