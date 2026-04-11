# GCS Bucket for Iceberg Data Storage
# Data is stored here as Parquet files, managed by BigLake Metastore
resource "google_storage_bucket" "lakehouse_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true # Useful for practice projects to clean up easily

  # Enforces modern IAM access control
  uniform_bucket_level_access = true
}

# BigQuery Dataset for Iceberg Tables
# This is where the Iceberg tables are registered in BigQuery
resource "google_bigquery_dataset" "lakehouse_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
}

# BigQuery Connection for BigLake
# This connection allows BigQuery to read/write Iceberg data from GCS
# BigLake Metastore manages the Iceberg table metadata automatically
resource "google_bigquery_connection" "lakehouse_conn" {
  connection_id = "biglake-connection"
  location      = var.region
  friendly_name = "Connection for Iceberg Tables via BigLake Metastore"
  cloud_resource {}
}

# IAM: Give the Connection permission to read/write the Bucket
# The BigLake connection service account needs storage access
resource "google_storage_bucket_iam_member" "connection_storage_object_admin" {
  bucket = google_storage_bucket.lakehouse_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_bigquery_connection.lakehouse_conn.cloud_resource[0].service_account_id}"
}

# NOTE: Iceberg tables are created via the ingestion pipeline using BigQuery SQL.
# They are automatically managed by BigLake Metastore and don't need to be
# defined in Terraform. The connection above is all that's required.
