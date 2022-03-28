resource "aws_ecr_repository" "this" {
  for_each = var.repositories
  name     = each.key
  tags     = var.tags
  image_scanning_configuration {
    scan_on_push = var.scan_on_push
  }
  image_tag_mutability = var.ecr_tag_immutability
}

resource "aws_ecr_lifecycle_policy" "this" {
  for_each   = var.repositories
  repository = each.key
  depends_on = [
  aws_ecr_repository.this]
  policy = <<EOF
{
    "rules": [
        {
            "rulePriority": 1,
            "description": "Expire untagged images older than ${var.ecr_untagged_expiry_days} days",
            "selection": {
                "tagStatus": "untagged",
                "countType": "sinceImagePushed",
                "countUnit": "days",
                "countNumber": ${var.ecr_untagged_expiry_days}
            },
            "action": {
                "type": "expire"
            }
        }
    ]
}
EOF
}
