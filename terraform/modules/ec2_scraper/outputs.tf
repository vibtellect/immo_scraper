/**
 * Outputs für EC2 Scraper Modul
 */

output "instance_id" {
  description = "ID der erstellten EC2-Instance"
  value       = aws_spot_instance_request.scraper.spot_instance_id
}

output "public_ip" {
  description = "Öffentliche IP-Adresse der EC2-Instance"
  value       = aws_eip.scraper.public_ip
}

output "security_group_id" {
  description = "ID der erstellten Sicherheitsgruppe"
  value       = aws_security_group.scraper_sg.id
}

output "iam_role_name" {
  description = "Name der erstellten IAM-Rolle"
  value       = aws_iam_role.scraper_role.name
}

output "iam_profile_name" {
  description = "Name des erstellten IAM-Instance-Profils"
  value       = aws_iam_instance_profile.scraper_profile.name
}
