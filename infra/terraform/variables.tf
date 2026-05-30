variable "aws_region" {
  description = "AWS region for the lakehouse bucket."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "S3 bucket name for StreamCommerce lakehouse."
  type        = string
  default     = "streamcommerce-lakehouse"
}
