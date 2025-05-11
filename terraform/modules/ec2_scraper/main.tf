/**
 * EC2 Scraper Module für kostengünstiges Web-Scraping
 * 
 * Konfiguriert eine t4g.nano/micro ARM-basierte EC2-Instance (Amazon Linux 2)
 * mit Spot-Instance-Preismodell für maximale Kosteneinsparung
 */

# AMI-Lookup für Amazon Linux 2 ARM
data "aws_ami" "amazon_linux_2_arm" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-arm64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Sicherheitsgruppe fuer minimalen Zugriff
resource "aws_security_group" "scraper_sg" {
  name        = "${var.name_prefix}-scraper-sg"
  description = "Security Group for Bazaraki Scraper EC2 Instance"
  vpc_id      = var.vpc_id

  # SSH-Zugriff nur von Ihrer IP-Adresse
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.allowed_ssh_cidr_blocks
  }

  # Ausgehender Traffic für Internet-Zugriff
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.name_prefix}-scraper-sg"
  }
}

# IAM-Rolle für die EC2-Instance
resource "aws_iam_role" "scraper_role" {
  name = "${var.name_prefix}-scraper-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

# IAM-Richtlinie für S3-Zugriff
resource "aws_iam_policy" "s3_access" {
  name        = "${var.name_prefix}-s3-access-policy"
  description = "Erlaubt EC2-Instance den Zugriff auf den S3-Bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Effect   = "Allow"
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}",
          "arn:aws:s3:::${var.s3_bucket_name}/*"
        ]
      }
    ]
  })
}

# IAM-Richtlinie für CloudWatch-Logs
resource "aws_iam_policy" "cloudwatch_logs" {
  name        = "${var.name_prefix}-cloudwatch-logs-policy"
  description = "Erlaubt EC2-Instance das Schreiben von CloudWatch-Logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Anhängen der Richtlinien an die Rolle
resource "aws_iam_role_policy_attachment" "s3_access_attach" {
  role       = aws_iam_role.scraper_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}

resource "aws_iam_role_policy_attachment" "cloudwatch_logs_attach" {
  role       = aws_iam_role.scraper_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

# Instance-Profil für EC2
resource "aws_iam_instance_profile" "scraper_profile" {
  name = "${var.name_prefix}-scraper-profile"
  role = aws_iam_role.scraper_role.name
}

# Spot-Instance-Anfrage für kostengünstige EC2
resource "aws_spot_instance_request" "scraper" {
  ami                         = data.aws_ami.amazon_linux_2_arm.id
  instance_type               = var.instance_type
  spot_price                  = var.spot_max_price
  spot_type                   = "persistent"
  wait_for_fulfillment        = true
  key_name                    = var.key_name
  vpc_security_group_ids      = [aws_security_group.scraper_sg.id]
  subnet_id                   = var.subnet_id
  iam_instance_profile        = aws_iam_instance_profile.scraper_profile.name
  instance_interruption_behavior = "stop"

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 8  # Minimaler Speicher
    delete_on_termination = true
  }

  # Userdata-Script zum Installieren von Abhängigkeiten
  user_data = <<-EOF
    #!/bin/bash
    # System-Updates
    yum update -y
    yum install -y amazon-cloudwatch-agent gcc gcc-c++ make git

    # Node.js 16 installieren
    curl -sL https://rpm.nodesource.com/setup_16.x | bash -
    yum install -y nodejs

    # Chromium und notwendige Abhängigkeiten
    amazon-linux-extras install epel -y
    yum install -y chromium

    # Crawlee und Puppeteer-Abhängigkeiten
    npm install -g pm2
    mkdir -p /home/ec2-user/bazaraki-scraper
    cd /home/ec2-user/bazaraki-scraper
    
    # Packetjs Repo
    cat > package.json << 'PACKAGE'
    {
      "name": "bazaraki-scraper",
      "version": "1.0.0",
      "description": "Bazaraki Property Scraper",
      "main": "index.js",
      "scripts": {
        "start": "node index.js"
      },
      "dependencies": {
        "puppeteer": "^19.7.5",
        "aws-sdk": "^2.1376.0",
        "axios": "^1.4.0",
        "cron": "^2.3.0"
      }
    }
    PACKAGE
    
    # Setup Berechtigungen
    chown -R ec2-user:ec2-user /home/ec2-user/bazaraki-scraper
    
    # Cronjob zur automatischen Ausführung
    echo "0 8,20 * * * cd /home/ec2-user/bazaraki-scraper && /usr/bin/node index.js > /var/log/scraper-cron.log 2>&1" | crontab -u ec2-user -
    
    # CloudWatch-Logs einrichten
    cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json << 'CWA'
    {
      "logs": {
        "logs_collected": {
          "files": {
            "collect_list": [
              {
                "file_path": "/var/log/scraper-cron.log",
                "log_group_name": "/ec2/bazaraki-scraper",
                "log_stream_name": "{instance_id}"
              }
            ]
          }
        }
      }
    }
    CWA
    
    systemctl start amazon-cloudwatch-agent
  EOF

  tags = {
    Name = "${var.name_prefix}-scraper"
  }

  # Schutz vor versehentlichem Löschen
  lifecycle {
    prevent_destroy = false
  }
}

# Elastic IP für SSH-Zugriff (optional, kann entfernt werden, wenn nicht benötigt)
resource "aws_eip" "scraper" {
  instance = aws_spot_instance_request.scraper.spot_instance_id
  vpc      = true
  
  tags = {
    Name = "${var.name_prefix}-scraper-eip"
  }
}
