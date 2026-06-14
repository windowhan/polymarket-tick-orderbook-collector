terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "collector_count" {
  description = "Number of collector instances"
  default     = 4
}

variable "collector_instance_type" {
  description = "EC2 instance type for collectors"
  default     = "t3.small"
}

variable "aggregator_instance_type" {
  description = "EC2 instance type for central aggregator"
  default     = "t3.medium"
}

variable "chunk_size" {
  description = "Tokens per WebSocket connection"
  default     = 100
}

variable "replication_factor" {
  description = "How many collectors each market is assigned to (for HA)"
  default     = 2
}

variable "heartbeat_timeout_secs" {
  description = "Seconds before a collector is considered stale"
  default     = 60
}

variable "delete_after_merge" {
  description = "Delete S3 objects after aggregator merges them"
  default     = false
}

variable "key_name" {
  description = "EC2 Key Pair name for SSH access"
  type        = string
}

variable "s3_bucket" {
  description = "S3 bucket for binary, markets.jsonl, and collector output"
  type        = string
}

variable "s3_prefix" {
  description = "S3 prefix for collector rotated files"
  default     = "orderbook/"
}

provider "aws" {
  region = var.region
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-22.04-amd64-server-*"]
  }
}

# Security Group: allow SSH in, 8080 between instances, everything out
resource "aws_security_group" "collector" {
  name_prefix = "polymarket-collector-"
  description = "Allow SSH inbound, inter-instance 8080, all outbound"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH"
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    self        = true
    description = "Aggregator orchestration API between collectors and aggregator"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound"
  }

  tags = {
    Name = "polymarket-collector"
  }
}

# IAM Role for S3 access (read binary/markets, write collector output, delete after merge)
resource "aws_iam_role" "collector" {
  name = "polymarket-collector-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "collector_s3" {
  name = "polymarket-collector-s3"
  role = aws_iam_role.collector.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${var.s3_bucket}",
        "arn:aws:s3:::${var.s3_bucket}/*"
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "collector" {
  name = "polymarket-collector-profile"
  role = aws_iam_role.collector.name
}

# Central Aggregator (must be created before collectors so IP is known)
resource "aws_instance" "aggregator" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.aggregator_instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.collector.id]
  iam_instance_profile   = aws_iam_instance_profile.collector.name

  root_block_device {
    volume_type = "gp3"
    volume_size = 20
    encrypted   = true
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    mode                   = "aggregator"
    s3_bucket              = var.s3_bucket
    s3_prefix              = var.s3_prefix
    region                 = var.region
    replication_factor     = var.replication_factor
    heartbeat_timeout_secs = var.heartbeat_timeout_secs
    delete_after_merge     = var.delete_after_merge
  })

  tags = {
    Name = "polymarket-aggregator"
  }
}

# Collectors
resource "aws_instance" "collector" {
  count                  = var.collector_count
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.collector_instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.collector.id]
  iam_instance_profile   = aws_iam_instance_profile.collector.name

  root_block_device {
    volume_type = "gp3"
    volume_size = 8
    encrypted   = true
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    mode            = "collector"
    s3_bucket       = var.s3_bucket
    s3_prefix       = var.s3_prefix
    region          = var.region
    chunk_size      = var.chunk_size
    aggregator_url  = "http://${aws_instance.aggregator.private_ip}:8080"
  })

  tags = {
    Name = "polymarket-collector-${count.index}"
  }
}

output "aggregator_private_ip" {
  value = aws_instance.aggregator.private_ip
}

output "aggregator_public_ip" {
  value = aws_instance.aggregator.public_ip
}

output "collector_private_ips" {
  value = aws_instance.collector[*].private_ip
}

output "collector_public_ips" {
  value = aws_instance.collector[*].public_ip
}
