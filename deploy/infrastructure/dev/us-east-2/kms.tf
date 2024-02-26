resource "aws_kms_alias" "kms_sti" {
  target_key_id = aws_kms_key.kms_sti.key_id
  name          = "alias/sti_flux"
}

resource "aws_kms_key" "kms_sti" {
  description = "Key used to encrypt storetheindex tenant secrets"
  policy      = data.aws_iam_policy_document.kms_sti.json
  is_enabled  = true

  tags = local.tags
}

data "aws_iam_policy_document" "kms_sti" {
  statement {
    sid = "Enable IAM User Permissions"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::407967248065:root"]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid = "Allow access for Devs via sops"

    principals {
      type = "AWS"

      identifiers = [
        "arn:aws:iam::407967248065:user/masih",
        "arn:aws:iam::407967248065:user/gammazero",
        "arn:aws:iam::407967248065:user/will.scott",
      ]
    }

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey"
    ]

    resources = ["*"]
  }


  statement {
    sid = "Allow Flux to decrypt"

    principals {
      type = "AWS"

      identifiers = [
        module.kustomize_controller_role.iam_role_arn
      ]
    }
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]
  }
}

data "aws_iam_policy_document" "kust_ctrlr" {
  statement {
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]

    resources = [aws_kms_key.kms_sti.arn, aws_kms_key.kms_cluster.arn, aws_kms_key.kms_index_provider.arn]
  }
}

resource "aws_iam_policy" "kust_ctrlr" {
  name   = "${local.environment_name}_kust_ctrlr"
  policy = data.aws_iam_policy_document.kust_ctrlr.json
  tags   = local.tags
}

module "kustomize_controller_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "5.20.0"

  create_role = true

  role_name    = "${local.environment_name}_kustomize_controller"
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
    aws_iam_policy.kust_ctrlr.arn,
  ]

  oidc_fully_qualified_subjects = ["system:serviceaccount:flux-system:kustomize-controller"]

  tags = local.tags
}

resource "aws_kms_alias" "kms_cluster" {
  target_key_id = aws_kms_key.kms_cluster.key_id
  name          = "alias${local.iam_path}cluster"
}

resource "aws_kms_key" "kms_cluster" {
  description = "Key used to encrypt cluster level secrets"
  policy      = data.aws_iam_policy_document.kms_cluster.json
  is_enabled  = true
}

data "aws_iam_policy_document" "kms_cluster" {
  statement {
    sid = "Enable IAM User Permissions"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::407967248065:root"]
    }

    actions   = ["kms:*"]
    resources = ["*"]
  }

  statement {
    sid = "Allow access for Devs via sops"

    principals {
      type = "AWS"

      identifiers = [
        "arn:aws:iam::407967248065:user/masih",
        "arn:aws:iam::407967248065:user/gammazero",
        "arn:aws:iam::407967248065:user/will.scott",
      ]
    }

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey"
    ]

    resources = ["*"]
  }

  statement {
    sid = "Allow Flux to decrypt"

    principals {
      type = "AWS"

      identifiers = [
        module.kustomize_controller_role.iam_role_arn
      ]
    }
    actions = [
      "kms:Decrypt",
      "kms:DescribeKey",
    ]
  }
}
