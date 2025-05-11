# Bazaraki Property Scraper

A Terraform-based AWS project that scans the real estate website twice daily to identify new and removed property listings and send notifications via Telegram.

## Key Features

- Node.js Lambda function that efficiently scans a real estate website
- Optimized scraping algorithm that compares existing Ad-IDs before scraping details
- Smart filtering for apartments and houses with customizable parameters
- S3 bucket for storing scraping results between runs
- CloudWatch Events for execution twice daily (8:00 and 20:00 UTC)
- Intelligent comparison between runs to identify new and removed listings
- Detailed Telegram notifications with images, prices, and property links

## Project Structure

```
immo_scraper/
├── README.md                     # Project documentation
├── .gitignore                    # Git exclusion list
├── build_lambda_nodejs_package.sh # Script to build Lambda package
├── lambda_function_nodejs.zip    # Packaged Lambda function
├── src/
│   ├── bazaraki_lambda_scraper.js # Main Node.js Lambda function
│   └── package.json              # Node.js dependencies
└── terraform/
    ├── main.tf                   # Main Terraform configuration
    ├── variables.tf              # Terraform variables
    ├── outputs.tf                # Output values
    └── terraform.tfvars          # Terraform variables values
```

## AWS Architecture (Terraform)

This project uses Terraform to define and provision the following AWS infrastructure:

![AWS Architecture](https://raw.githubusercontent.com/username/immo_scraper/main/docs/architecture.png)

### Infrastructure Components

#### AWS Lambda Function
```hcl
resource "aws_lambda_function" "scraper" {
  function_name     = local.lambda_function_name
  filename          = local.lambda_nodejs_zip_path
  source_code_hash  = filebase64sha256(local.lambda_nodejs_zip_path)
  handler           = "src/bazaraki_lambda_scraper.handler"
  runtime           = "nodejs16.x"
  role              = aws_iam_role.lambda_role.arn
  timeout           = 300  # 5 minutes
  memory_size       = 256  # MB
  
  environment {
    variables = {
      S3_BUCKET_NAME     = aws_s3_bucket.scraper_results.bucket
      TELEGRAM_BOT_TOKEN = var.telegram_bot_token
      TELEGRAM_CHAT_ID   = var.telegram_chat_id
    }
  }
}
```

#### S3 Bucket for Storing Results
```hcl
resource "aws_s3_bucket" "scraper_results" {
  bucket = local.s3_bucket_name
  # Configured with private ACL and appropriate ownership controls
}
```

#### CloudWatch Event Rules (Scheduled Triggers)
```hcl
resource "aws_cloudwatch_event_rule" "schedule_1" {
  name                = "${local.project_prefix}-schedule-1"
  description         = "Trigger the property scraper Lambda - Morning run"
  schedule_expression = "cron(0 8 * * ? *)"
}

resource "aws_cloudwatch_event_rule" "schedule_2" {
  name                = "${local.project_prefix}-schedule-2"
  description         = "Trigger the property scraper Lambda - Evening run"
  schedule_expression = "cron(0 20 * * ? *)"
}
```

#### SNS Topic for Notifications
```hcl
resource "aws_sns_topic" "property_notifications" {
  name = "${local.project_prefix}-notifications"
}
```

#### IAM Role and Policies
```hcl
resource "aws_iam_role" "lambda_role" {
  name = "${local.project_prefix}-lambda-role"
  # Assume role policy for Lambda
}

resource "aws_iam_policy" "s3_access" {
  name        = "${local.project_prefix}-s3-access"
  description = "Allow Lambda to access the S3 bucket"
  # Policy document with S3 permissions
}

resource "aws_iam_policy" "sns_access" {
  name        = "${local.project_prefix}-sns-access"
  description = "Allow Lambda to publish to SNS topics"
  # Policy document with SNS permissions
}
```

## Installation

### Prerequisites

- Python 3.9+
- AWS CLI configured
- Terraform 1.0+
- A Telegram bot (created via @BotFather)

### Setup

1. **Clone the repository**

```bash
git clone <repository-url>
cd immo_scraper
```

2. **Create and activate a virtual environment**

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**

```bash
pip install -r src/requirements.txt
```

4. **Configure environment variables**

Copy the `.env.example` file to `.env` and update the values:

```bash
cp .env.example .env
# Edit .env with your preferred editor
```

5. **Configure Terraform**

Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars` and update the values:

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform/terraform.tfvars with your preferred editor
```

## Usage

### Local Testing

**Test the Telegram notification function:**

```bash
python test_telegram.py
```

**Simulate an AWS Lambda invocation locally:**

```bash
python test_lambda.py
```

### Deployment to AWS

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

### Manual Execution of the Lambda Function

**Via the AWS Management Console:**

1. Open the [AWS Lambda Console](https://console.aws.amazon.com/lambda)
2. Select your function from the list
3. Click on "Test" and create a test event
4. Click "Test" to run the function

**With the AWS CLI:**

```bash
aws lambda invoke \
  --function-name vibtellect-immo-scraper \
  --payload '{"config":{"location":"paphos","deal_type":"rent"}}' \
  output.json
```

## Telegram Notifications

After each scan (at 8:00 and 20:00 UTC) or during manual execution, a Telegram message is sent with the following information:

- Number of property listings found
- List of new listings (with links)
- List of removed listings

## Customization

You can customize the following parameters:

- **Filters**: In the Lambda function or manual invocations, you can adjust the filters for location, price, and bedrooms
- **Schedule**: In the Terraform configuration, you can adjust the CloudWatch event rules
- **Notifications**: In the Lambda function, you can customize the format of Telegram messages

## Security Notes

- Sensitive data such as API tokens should never be stored in plain text in the repository
- Use the `.env` file for local tests and AWS Secrets Manager for deployment
- The `.gitignore` file is configured to exclude sensitive files

## License

This project is licensed under the MIT License.
