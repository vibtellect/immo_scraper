/**
 * Variables für EC2 Scraper Modul
 */

variable "name_prefix" {
  description = "Präfix für Ressourcennamen"
  type        = string
  default     = "bazaraki"
}

variable "vpc_id" {
  description = "ID des VPC, in dem die EC2-Instance erstellt werden soll"
  type        = string
}

variable "subnet_id" {
  description = "ID des Subnets, in dem die EC2-Instance erstellt werden soll"
  type        = string
}

variable "key_name" {
  description = "Name des SSH-Key-Pairs für den Zugriff auf die EC2-Instance"
  type        = string
  default     = null
}

variable "allowed_ssh_cidr_blocks" {
  description = "CIDR-Blöcke mit SSH-Zugriff auf die EC2-Instance"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # Sicherheitshinweis: In Produktion einschränken!
}

variable "instance_type" {
  description = "EC2-Instance-Typ"
  type        = string
  default     = "t4g.micro"  # ARM-basiert für bessere Preis-Leistung
}

variable "spot_max_price" {
  description = "Maximaler Preis für Spot-Instance (leer lassen für aktuellen Spot-Preis)"
  type        = string
  default     = ""  # Leerer String bedeutet aktueller Spot-Preis
}

variable "s3_bucket_name" {
  description = "Name des S3-Buckets für die Speicherung der Scraper-Ergebnisse"
  type        = string
}
