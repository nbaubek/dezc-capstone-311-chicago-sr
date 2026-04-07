# GCS Bucket for Iceberg Storage
resource "google_storage_bucket" "lakehouse_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true # Useful for practice projects to clean up easily

  # ADD THIS LINE: Enforces modern IAM access control
  uniform_bucket_level_access = true
}

# BigQuery Dataset
resource "google_bigquery_dataset" "lakehouse_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
}

# BigQuery Connection (The "Bridge")
resource "google_bigquery_connection" "lakehouse_conn" {
  connection_id = "biglake-connection"
  location      = var.region
  friendly_name = "Connection for Iceberg Tables"
  cloud_resource {}
}

# IAM: Give the Connection permission to read the Bucket
resource "google_storage_bucket_iam_member" "connection_storage_viewer" {
  bucket = google_storage_bucket.lakehouse_bucket.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_bigquery_connection.lakehouse_conn.cloud_resource[0].service_account_id}"
}


# 8. BigLake Table (Initial Skeleton)
# Note: You'll typically manage specific table creation/updates 
# via PyIceberg during ingestion, but here is the definition for the catalog.
# resource "google_bigquery_table" "chicago_311_iceberg" {
#   dataset_id          = google_bigquery_dataset.lakehouse_dataset.dataset_id
#   table_id            = "chicago_311"
#   deletion_protection = false # Set to true in real production

#   external_data_configuration {
#     # Link to the connection created in Step 3
#     connection_id = google_bigquery_connection.lakehouse_conn.name
#     source_format = "ICEBERG"
    
#     # THE CATCH: This points to the metadata JSON file created by PyIceberg.
#     source_uris   = ["gs://${google_storage_bucket.lakehouse_bucket.name}/metadata/v1.metadata.json"]
#   }

#   # PRO-TIP: Prevent Terraform from overriding PyIceberg
#   lifecycle {
#     ignore_changes = [
#       external_data_configuration[0].source_uris
#     ]
#   }
# }