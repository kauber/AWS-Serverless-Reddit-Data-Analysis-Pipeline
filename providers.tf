terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0" # Use a recent version
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.2"
    }
  }
  required_version = ">= 1.0"
}

provider "aws" {
  region  = var.aws_region
  profile = "developer" # Tells Terraform to use the 'developer' profile
}

# Fetch current AWS account ID and region dynamically
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}