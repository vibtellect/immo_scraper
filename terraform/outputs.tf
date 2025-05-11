output "lambda_function_name" {
  description = "Name der Lambda-Funktion"
  value       = aws_lambda_function.scraper.function_name
}

output "lambda_function_arn" {
  description = "ARN der Lambda-Funktion"
  value       = aws_lambda_function.scraper.arn
}

output "s3_bucket_name" {
  description = "Name des S3-Buckets für die Ergebnisse"
  value       = aws_s3_bucket.scraper_results.bucket
}

output "cron_schedule_1" {
  description = "Erste tägliche Ausführungszeit"
  value       = var.schedule_expression_1
}

output "cron_schedule_2" {
  description = "Zweite tägliche Ausführungszeit"
  value       = var.schedule_expression_2
}

output "sns_topic_arn" {
  description = "ARN des SNS-Topics für Benachrichtigungen"
  value       = aws_sns_topic.property_notifications.arn
}
