locals {
  saturn_lassie_tags = {
    "Environment" = local.environment_name
    "ManagedBy"   = "terraform"
    Owner         = "saturn-lassie"
    Team          = "bedrock/tornado"
    Organization  = "EngRes"
  }
}

module "saturn_lassie_events_db" {
  source  = "registry.terraform.io/terraform-aws-modules/rds/aws"
  version = "5.6.0"

  identifier = "${local.environment_name}-saturn-lassie-events"

  engine               = "postgres"
  engine_version       = "14.7"
  family               = "postgres14"
  major_engine_version = "14"
  instance_class       = "db.m5.large"

  allocated_storage   = 2500
  skip_final_snapshot = true
  db_name             = "SaturnLassieEvents"
  username            = "postgres"
  port                = 5432

  multi_az               = true
  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [module.saturn_lassie_events_db_sg.security_group_id]

  maintenance_window = "Mon:00:00-Mon:03:00"
  backup_window      = "03:00-06:00"

  deletion_protection       = true
  create_db_option_group    = false
  create_db_parameter_group = false

  tags = local.saturn_lassie_tags
}

module "saturn_lassie_events_db_sg" {
  source  = "registry.terraform.io/terraform-aws-modules/security-group/aws"
  version = "~> 4.0"

  name        = "${local.environment_name}-saturn-lassie-events-db"
  description = "Saturn Lassie events db security group"
  vpc_id      = module.vpc.vpc_id

  # ingress
  ingress_with_cidr_blocks = [
    {
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      description = "PostgreSQL access from within VPC"
      cidr_blocks = module.vpc.vpc_cidr_block
    }
  ]

  tags = local.saturn_lassie_tags
}
