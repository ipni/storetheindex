locals {
  # The table must have a primary key named LockID.
  # See below for more detail.
  # https://www.terraform.io/docs/backends/types/s3.html#dynamodb_table
  lock_key_id = "LockID"
}

resource "aws_dynamodb_table" "ipni_gh_mgmt_lock" {
  name         = "ipni_gh_mgmt_lock"
  hash_key     = local.lock_key_id
  billing_mode = "PAY_PER_REQUEST"

  attribute {
    name = local.lock_key_id
    type = "S"
  }
  tags = local.tags
}

module "ipni_gh_mgmt_ro" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-user"
  version = "4.18.0"

  name                    = "ipni_gh_mgmt_ro"
  path                    = local.iam_path
  pgp_key                 = local.pgp_key
  password_reset_required = false
  create_iam_access_key   = true
  tags                    = local.tags
}
module "ipni_gh_mgmt_rw" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-user"
  version = "4.18.0"

  name                    = "ipni_gh_mgmt_rw"
  path                    = local.iam_path
  pgp_key                 = local.pgp_key
  password_reset_required = false
  create_iam_access_key   = true
  tags                    = local.tags
}

data "aws_iam_policy_document" "ipni_gh_mgmt_ro" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::sti-dev-terraform-state"]
  }
  statement {
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::sti-dev-terraform-state/github-mgmt/*"]
  }
  statement {
    effect    = "Allow"
    actions   = ["dynamodb:GetItem"]
    resources = ["arn:aws:dynamodb:*:*:table/${aws_dynamodb_table.ipni_gh_mgmt_lock.name}"]
  }
}

data "aws_iam_policy_document" "ipni_gh_mgmt_rw" {
  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::sti-dev-terraform-state"]
  }
  statement {
    effect  = "Allow"
    actions = [
      "s3:PutObject",
      "s3:GetObject",
      "s3:DeleteObject"
    ]
    resources = ["arn:aws:s3:::sti-dev-terraform-state/github-mgmt/*"]
  }
  statement {
    effect  = "Allow"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:DeleteItem"
    ]
    resources = ["arn:aws:dynamodb:*:*:table/${aws_dynamodb_table.ipni_gh_mgmt_lock.name}"]
  }
}

resource "aws_iam_user_policy" "gh_mgmt_ro" {
  user   = module.ipni_gh_mgmt_ro.iam_user_name
  name   = "${local.environment_name}_ipni_gh_mgmt_ro"
  policy = data.aws_iam_policy_document.ipni_gh_mgmt_ro.json
}

resource "aws_iam_user_policy" "gh_mgmt_rw" {
  user   = module.ipni_gh_mgmt_rw.iam_user_name
  name   = "${local.environment_name}_ipni_gh_mgmt_rw"
  policy = data.aws_iam_policy_document.ipni_gh_mgmt_rw.json
}
