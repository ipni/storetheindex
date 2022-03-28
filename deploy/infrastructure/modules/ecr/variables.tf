variable "repositories" {
  type        = set(string)
  description = "The list of namespace and repository names in `<namespace>/<repo-name>` format."
}

variable "tags" {
  type        = map(string)
  description = "The tags applied to all created AWS resources."
}

variable "ecr_untagged_expiry_days" {
  type        = number
  description = "The number of days after which to expire untagged pushed images"
  default     = 7
}

variable "ecr_tag_immutability" {
  type        = string
  description = "The immutability of container image tags published to ECR repository, either `MUTABLE` or `IMMUTABLE`."
  default     = "IMMUTABLE"
}

variable "scan_on_push" {
  type        = bool
  description = "Indicates whether images are scanned after being pushed to the repository."
  default     = true
}
