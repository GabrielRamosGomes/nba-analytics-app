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