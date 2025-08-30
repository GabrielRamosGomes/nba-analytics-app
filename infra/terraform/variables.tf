variable "aws_region" {
    description = "The AWS region to deploy resources in"
    type        = string
    default     = "eu-west-3"
}

variable "project_name" {
    description = "Name of the project"
    type        = string
    default     = "nba-analytics"
}

variable "aws_access_key" {
    description = "AWS access key"
    type        = string
}

variable "aws_secret_key" {
    description = "AWS secret key"
    type        = string
}

variable "environment" {
    description = "Deployment environment (e.g., dev, prod)"
    type        = string
    default     = "dev"
}