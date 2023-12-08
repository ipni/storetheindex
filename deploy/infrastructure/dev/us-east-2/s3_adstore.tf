resource "aws_s3_bucket" "sti_s3_adstore" {
  bucket = "${local.environment_name}-sti-adstore"
  tags   = local.tags
}


resource "aws_s3_bucket_acl" "adstore" {
  bucket = aws_s3_bucket.sti_s3_adstore.id
  acl    = "private"
}

data "aws_iam_policy_document" "sti_s3_rw" {
  statement {
    effect = "Allow"

    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]

    resources = [aws_s3_bucket.sti_s3_adstore.arn, "${aws_s3_bucket.sti_s3_adstore.arn}/*"]
  }
}

resource "aws_iam_policy" "sti_s3_rw" {
  name   = "${local.environment_name}_sti_s3_rw"
  policy = data.aws_iam_policy_document.sti_s3_rw.json
  tags   = local.tags
}


module "sti_s3_rw_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.17.1"

  create_role = true

  role_name    = "${local.environment_name}_sti_s3_rw"
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.sti_s3_rw.arn
  ]

  oidc_fully_qualified_subjects = ["system:serviceaccount:storetheindex:storetheindex"]

  tags = local.tags
}
