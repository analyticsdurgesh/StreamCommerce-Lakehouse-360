output "lakehouse_bucket" {
  value = aws_s3_bucket.lakehouse.bucket
}

output "lakehouse_uri" {
  value = "s3://${aws_s3_bucket.lakehouse.bucket}"
}
