locals {
  # ECR public repositories will be added to the `storetheindex` ECR registry under:
  #  - public.ecr.aws/storetheindex/
  ecr_public_repositories = {
    index-provider : {
      about_text        = "Index Provider to Network Indexer Protocol"
      architectures     = ["amd64"]
      description       = "https://github.com/ipni/index-provider"
      operating_systems = ["Linux"]
    }
  }
}

resource "aws_ecrpublic_repository" "this" {
  # ECR public repositories are only available in us-east-1.
  provider        = aws.use1
  for_each        = local.ecr_public_repositories
  repository_name = each.key

  catalog_data {
    about_text        = lookup(each.value, "about_text", "")
    architectures     = lookup(each.value, "architectures", [])
    description       = lookup(each.value, "description", "")
    operating_systems = lookup(each.value, "operating_systems", [])
    usage_text        = lookup(each.value, "usage_text", "")
    logo_image_blob   = lookup(each.value, "logo_image_blob", "")
  }
}
