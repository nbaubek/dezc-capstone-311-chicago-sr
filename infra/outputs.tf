output "ENV_GCP_PROJECT_ID" {
  value       = var.project_id
  description = "Paste this as GCP_PROJECT_ID in your .env"
}

output "ENV_LAKEHOUSE_BUCKET" {
  value       = "gs://${google_storage_bucket.lakehouse_bucket.name}/metadata"
  description = "Paste this as LAKEHOUSE_BUCKET in your .env (for Nessie catalog + GCS storage)"
}

# Note: Nessie REST catalog is used for catalog management
# BigLake resources are only needed for BigQuery integration later