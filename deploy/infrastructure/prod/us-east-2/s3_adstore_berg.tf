locals {
  berg_username = "berg"
}

module "berg_user" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-user"
  version = "5.20.0"

  name                          = local.berg_username
  pgp_key                       = local.pgp_key
  create_iam_user_login_profile = false
  create_iam_access_key         = true
}

data "aws_iam_policy_document" "berg_s3_ro" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:ListBucket",
    ]

    resources = [aws_s3_bucket.sti_s3_adstore.arn, "${aws_s3_bucket.sti_s3_adstore.arn}/*"]
  }
}

resource "aws_iam_policy" "berg_s3_ro" {
  name   = "${local.environment_name}_berg_s3_ro"
  policy = data.aws_iam_policy_document.berg_s3_ro.json
}

resource "aws_iam_user_policy_attachment" "berg_s3_ro" {
  policy_arn = aws_iam_policy.berg_s3_ro.arn
  user       = local.berg_username
}