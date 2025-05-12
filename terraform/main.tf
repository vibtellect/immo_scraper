resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# SNS Topic für Benachrichtigungen
resource "aws_sns_topic" "property_notifications" {
  name = "${var.project_name}-notifications"
}

# E-Mail-Abonnement für SNS-Benachrichtigungen
resource "aws_sns_topic_subscription" "email_subscription" {
  count     = var.notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.property_notifications.arn
  protocol  = "email"
  endpoint  = var.notification_email
}

# IAM-Richtlinie für SNS-Zugriff
resource "aws_iam_policy" "sns_access" {
  name        = "${var.project_name}-sns-access"
  description = "Erlaubt Lambda Zugriff auf SNS Topic"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "sns:Publish",
          "sns:ListSubscriptionsByTopic"
        ]
        Effect   = "Allow"
        Resource = aws_sns_topic.property_notifications.arn
      }
    ]
  })
}

# Anhängen der SNS-Richtlinie an die Lambda-Rolle
resource "aws_iam_role_policy_attachment" "lambda_sns" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.sns_access.arn
}

# S3 Bucket für die Speicherung der Scraping-Ergebnisse
resource "aws_s3_bucket" "scraper_results" {
  bucket = "${var.project_name}-results"
}

# Aktiviere ACLs für den Bucket (erforderlich für neuere AWS-Konfigurationen)
resource "aws_s3_bucket_ownership_controls" "scraper_results_ownership" {
  bucket = aws_s3_bucket.scraper_results.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

# Setze die ACL auf private
resource "aws_s3_bucket_acl" "scraper_results_acl" {
  depends_on = [aws_s3_bucket_ownership_controls.scraper_results_ownership]
  bucket = aws_s3_bucket.scraper_results.id
  acl    = "private"
}

resource "aws_iam_policy" "s3_access" {
  name        = "${var.project_name}-s3-access"
  description = "Erlaubt Lambda Zugriff auf S3 Bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.scraper_results.arn,
          "${aws_s3_bucket.scraper_results.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}

# Lambda-Funktion für das Scraping mit Node.js
# Wir verwenden das vorbereitete ZIP-Paket aus dem build_lambda_nodejs_package.sh Skript
locals {
  lambda_nodejs_zip_path = "${path.module}/../lambda_function_nodejs.zip"
}

resource "aws_lambda_function" "scraper" {
  function_name    = var.project_name
  role             = aws_iam_role.lambda_role.arn
  handler          = "src/bazaraki_lambda_scraper.handler"
  runtime          = "nodejs18.x"
  filename         = local.lambda_nodejs_zip_path
  source_code_hash = filebase64sha256(local.lambda_nodejs_zip_path)
  timeout          = 900  # Erhöht auf das Maximum von 15 Minuten
  memory_size      = 512

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.scraper_results.bucket,
      RESULTS_PREFIX = "results/",
      TELEGRAM_BOT_TOKEN = var.telegram_bot_token,
      TELEGRAM_CHAT_ID = var.telegram_chat_id,
      DEBUG_MODE = "false",
      FORCE_NOTIFICATION = "false" # Nur für den ersten Lauf auf "true" setzen
    }
  }
}

# CloudWatch Events/EventBridge für die geplante Ausführung
resource "aws_cloudwatch_event_rule" "schedule_1" {
  name                = "${var.project_name}-schedule-1"
  description         = "Erste tägliche Ausführung des Scrapers"
  schedule_expression = var.schedule_expression_1
}

resource "aws_cloudwatch_event_rule" "schedule_2" {
  name                = "${var.project_name}-schedule-2"
  description         = "Zweite tägliche Ausführung des Scrapers"
  schedule_expression = var.schedule_expression_2
}

resource "aws_cloudwatch_event_target" "scraper_target_1" {
  rule      = aws_cloudwatch_event_rule.schedule_1.name
  target_id = "${var.project_name}-target-1"
  arn       = aws_lambda_function.scraper.arn
}

resource "aws_cloudwatch_event_target" "scraper_target_2" {
  rule      = aws_cloudwatch_event_rule.schedule_2.name
  target_id = "${var.project_name}-target-2"
  arn       = aws_lambda_function.scraper.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_1" {
  statement_id  = "AllowExecutionFromCloudWatch1"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.scraper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_1.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_2" {
  statement_id  = "AllowExecutionFromCloudWatch2"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.scraper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule_2.arn
}
