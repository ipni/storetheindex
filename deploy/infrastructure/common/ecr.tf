resource "aws_ecrpublic_repository" "storetheindex" {
  repository_name = "storetheindex"
  # Public ECR is only supported on us-east-1 region.
  provider        = aws.use1
  catalog_data {
    about_text        = "storetheindex"
    operating_systems = ["linux"]
    architectures     = ["amd64"]
  }
}
