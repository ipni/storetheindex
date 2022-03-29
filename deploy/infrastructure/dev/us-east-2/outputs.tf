output "kms_sti_flux_arn" {
  value = aws_kms_alias.kms_sti.arn
}

output "kustomize_controller_role_arn" {
  value = module.kustomize_controller_role.iam_role_arn
}
