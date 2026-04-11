"""
Tasks module for Chicago 311 data pipeline.

This module contains reusable Prefect tasks for:
- Extracting and loading data from Socrata API to BigQuery/BigLake
- WAP (Write-Audit-Publish) helpers using audit table swap pattern
"""

# BigQuery BigLake ingestion tasks
from flows.tasks.bigquery_ingestion import (
    extract_and_load_chunk,
    ensure_dataset_exists,
    ensure_iceberg_table_exists,
    create_audit_table,
    clear_audit_table,
    audit_table_checks,
    replace_main_with_audit,
)

__all__ = [
    "extract_and_load_chunk",
    "ensure_dataset_exists",
    "ensure_iceberg_table_exists",
    "create_audit_table",
    "clear_audit_table",
    "audit_table_checks",
    "replace_main_with_audit",
]
