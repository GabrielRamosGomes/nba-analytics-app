# -------------------------------
# S3 Bucket for NBA Data Storage
# -------------------------------
resource "aws_s3_bucket" "nba_data_bucket" {
    bucket = "${var.project_name}-data-${var.environment}"
}

resource "aws_s3_bucket_versioning" "nba_data_versioning" {
    bucket = aws_s3_bucket.nba_data_bucket.id
    versioning_configuration {
        status = "Enabled"
    }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "nba_data_encryption" {
    bucket = aws_s3_bucket.nba_data_bucket.id
    rule {
        apply_server_side_encryption_by_default {
            sse_algorithm = "AES256"
        }
    }
}

# Block public access to the S3 bucket
resource "aws_s3_bucket_public_access_block" "nba_data_public_access" {
    bucket = aws_s3_bucket.nba_data_bucket.id

    block_public_acls       = true
    block_public_policy     = true
    ignore_public_acls      = true
    restrict_public_buckets = true
}

# -------------------------------
# IAM User and Policy for S3 Access
# -------------------------------
resource "aws_iam_user" "nba_data_user" {
  name = "${var.project_name}-data-user"
}

resource "aws_iam_access_key" "nba_data_access_key" {
    user = aws_iam_user.nba_data_user.name
}

resource "aws_iam_policy" "nba_s3_policy" {
    name        = "${var.project_name}-s3-policy"
    description = "Allow access to the NBA S3 bucket"

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
        {
            Effect = "Allow"
            Action = [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ]
            Resource = [
                aws_s3_bucket.nba_data_bucket.arn,
                "${aws_s3_bucket.nba_data_bucket.arn}/*"
            ]
        }
        ]
    })
}

# Attach the policy to the IAM user
resource "aws_iam_user_policy_attachment" "nba_user_policy_attachment" {
    user       = aws_iam_user.nba_data_user.name
    policy_arn = aws_iam_policy.nba_s3_policy.arn
}

# -------------------------------
# Outputs
# -------------------------------
output "s3_bucket_name" {
    description = "S3 bucket name for NBA data"
    value       = aws_s3_bucket.nba_data_bucket.bucket
}
output "aws_region" {
    description = "AWS region"
    value       = var.aws_region
}

output "aws_access_key_id" {
    description = "AWS Access Key ID for programmatic access"
    value       = aws_iam_access_key.nba_data_access_key.id
}

output "s3_bucket_arn" {
    description = "S3 bucket ARN"
    value       = aws_s3_bucket.nba_data_bucket.arn
}