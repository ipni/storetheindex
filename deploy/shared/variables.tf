variable "app" {
  description = "The name of the application"
  type        = string
}

variable "allowed_account_id" {
  description = "account id used for AWS"
  type = string
}

variable "region" {
  description = "aws region for all services"
  type        = string
}

variable "domain_base" {
  type = string
  default = ""
}


variable "cloudflare_zone_id" {
  type = string
}
