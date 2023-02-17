resource "aws_s3_bucket" "adstore" {
  bucket = "adstore-bucket"

  tags = {
    Name        = "advertisement car bucket"
    Environment = "dev"
  }
}

resource "aws_s3_bucket_acl" "adstore" {
  bucket = aws_s3_bucket.adstore.id
  acl    = "private"
}

resource "aws_s3_bucket_acl" "adstore" {
  bucket = aws_s3_bucket.adstore.id
  access_control_policy {
    grant {
      grantee {
        id   = data.aws_canonical_user_id.current.id
        type = "CanonicalUser"
      }
      permission = "READ"
    }
    grant {
      owner {
        id = data.aws_canonical_user_id.current.id
      }
    }
  }
}
