resource "aws_kms_alias" "kms_index_provider" {
  target_key_id = aws_kms_key.kms_index_provider.key_id
  name          = "alias${local.iam_path}index_provider"
}

resource "aws_kms_key" "kms_index_provider" {
  description = "Key used to encrypt index_provider tenant secrets"
  policy      = data.aws_iam_policy_document.kms_index_provider.json
  is_enabled  = true

  tags = local.tags
}

data "aws_iam_policy_document" "kms_index_provider" {
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
