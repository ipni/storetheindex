resource "aws_kms_alias" "kms_autoretrieve" {
  target_key_id = aws_kms_key.kms_autoretrieve.key_id
  name          = "alias${local.iam_path}autoretrieve"
}

resource "aws_kms_key" "kms_autoretrieve" {
  description = "Key used to encrypt autoretrieve tenant secrets"
  policy      = data.aws_iam_policy_document.kms_autoretrieve.json
  is_enabled  = true

  tags = local.tags
}

data "aws_iam_policy_document" "kms_autoretrieve" {
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
        "arn:aws:iam::407967248065:user/marco",
        "arn:aws:iam::407967248065:user/gammazero",
        "arn:aws:iam::407967248065:user/will.scott",
        "arn:aws:iam::407967248065:user/kylehuntsman",
        "arn:aws:iam::407967248065:user/steveFraser",
        "arn:aws:iam::407967248065:user/cmharden",
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
