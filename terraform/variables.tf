variable "aws_region" {
  description = "AWS Region für die Ressourcen"
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Name des Projekts"
  type        = string
  default     = "vibtellect-immo-scraper"
}

# Allgemeine Konfiguration
variable "name_prefix" {
  description = "Präfix für Namen der AWS-Ressourcen"
  type        = string
  default     = "bazaraki"
}

# Lambda-Konfiguration
variable "schedule_expression_1" {
  description = "Cron-Expression für die erste tägliche Ausführung"
  type        = string
  default     = "cron(0 8 * * ? *)"  # Jeden Tag um 8:00 UTC
}

variable "schedule_expression_2" {
  description = "Cron-Expression für die zweite tägliche Ausführung"
  type        = string
  default     = "cron(0 20 * * ? *)" # Jeden Tag um 20:00 UTC
}

# Benachrichtigungskonfiguration
variable "notification_email" {
  description = "E-Mail-Adresse für Benachrichtigungen"
  type        = string
  default     = ""  # Dies muss bei der Anwendung von Terraform überschrieben werden
}

variable "telegram_bot_token" {
  description = "Telegram Bot-Token für Benachrichtigungen"
  type        = string
  default     = ""  # Dies muss bei der Anwendung von Terraform überschrieben werden
  sensitive   = true  # Als sensitiv markiert, damit es nicht in Logs erscheint
}

variable "telegram_chat_id" {
  description = "Telegram Chat-ID für Benachrichtigungen"
  type        = string
  default     = ""  # Dies muss bei der Anwendung von Terraform überschrieben werden
}

# EC2-Scraper Konfiguration
variable "ec2_instance_type" {
  description = "EC2-Instance-Typ für den Scraper"
  type        = string
  default     = "t4g.micro"  # ARM-basiert für bessere Preis-Leistung
}

variable "ec2_spot_max_price" {
  description = "Maximaler Preis für EC2 Spot-Instance (leer = aktueller Spot-Preis)"
  type        = string
  default     = ""  # Leerer String verwendet den aktuellen Spot-Preis
}

variable "ssh_public_key" {
  description = "SSH Public Key für den Zugriff auf die EC2-Instance"
  type        = string
  default     = ""  # Falls leer, wird ~/.ssh/id_rsa.pub verwendet
}

variable "allowed_ssh_cidr_blocks" {
  description = "CIDR-Blöcke mit SSH-Zugriff auf die EC2-Instance"
  type        = list(string)
  default     = ["0.0.0.0/0"]  # In Produktion einschränken!
}
