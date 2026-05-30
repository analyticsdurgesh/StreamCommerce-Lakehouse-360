resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "folders" {
  for_each = toset([
    "landing/customer_events/",
    "landing/orders/",
    "landing/payments/",
    "landing/inventory_updates/",
    "landing/product_catalog_updates/",
    "landing/shipment_events/",
    "bronze/",
    "silver/",
    "gold/",
    "quarantine/invalid_records/",
    "quarantine/schema_violations/",
    "quarantine/dq_failures/"
  ])
  bucket  = aws_s3_bucket.lakehouse.id
  key     = each.value
  content = ""
}
