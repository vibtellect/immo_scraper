terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
  required_version = ">= 1.0.0"
}

provider "aws" {
  region = var.aws_region
  
  # Falls du spezifische Anmeldedaten hast, kannst du diese hier eintragen
  # profile = "default"
}
