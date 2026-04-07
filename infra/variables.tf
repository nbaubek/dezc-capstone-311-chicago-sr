variable "project_id" {
  description = "The Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for resources (e.g., US or us-central1)"
  type        = string
  default     = "US"
}

variable "bucket_name" {
  description = "The name of the GCS bucket for the Iceberg Lakehouse"
  type        = string
}

variable "dataset_id" {
  description = "The BigQuery dataset ID for BigLake tables"
  type        = string
}