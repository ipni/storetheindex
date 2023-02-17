#resource "aws_s3_bucket" "adstore" {
#  bucket = "${local.environment_name}-sti-adstore"
#
#  tags = local.tags
#}
#
#resource "aws_s3_bucket_acl" "adstore" {
#  bucket = aws_s3_bucket.adstore.id
#  acl    = "private"
#}

module "sti_s3_rw_role" {
  source  = "registry.terraform.io/terraform-aws-modules/iam/aws//modules/iam-assumable-role-with-oidc"
  version = "4.17.1"

  create_role = true

  role_name    = "${local.environment_name}_sti_s3_rw"
  provider_url = module.eks.oidc_provider

  role_policy_arns = [
#    TODO find predefined ARN
  ]

  oidc_fully_qualified_subjects = ["system:serviceaccount:storetheindex:storetheindex"]

  tags = local.tags
}


#resource "aws_s3_bucket_acl" "adstore" {
#  bucket = aws_s3_bucket.adstore.id
#  access_control_policy {
#    grant {
#      grantee {
#        id   = data.aws_canonical_user_id.current.id
#        type = "CanonicalUser"
#      }
#      permission = "READ"
#    }
#    grant {
#      owner {
#        id = data.aws_canonical_user_id.current.id
#      }
#    }
#  }
#}
