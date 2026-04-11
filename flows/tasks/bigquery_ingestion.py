"""
BigQuery BigLake ingestion module for Chicago 311 service requests.

This module replaces PyIceberg with BigQuery's native Iceberg support via BigLake.
Uses audit table swap pattern for WAP (Write-Audit-Publish).
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import polars as pl
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from prefect import task, get_run_logger
from sodapy import Socrata

# Table configuration
TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')}.service_requests"
AUDIT_TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')}.service_requests_audit"
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')
CONNECTION_NAME = "biglake-connection"  # From Terraform

# Socrata configuration
SOCRATA_DATASET = "v6vf-nfxy"
SOCRATA_APP_TOKEN = os.getenv('SOCRATA_APP_TOKEN')


def _get_bigquery_client() -> bigquery.Client:
    """Get a BigQuery client with default credentials."""
    return bigquery.Client(project=os.getenv('GCP_PROJECT_ID'))


@task(retries=3, retry_delay_seconds=30)
def ensure_dataset_exists() -> None:
    """Ensure the BigQuery dataset exists for Iceberg tables."""
    logger = get_run_logger()
    client = _get_bigquery_client()

    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {DATASET_ID} already exists")
    except Exception:
        logger.info(f"Creating dataset {DATASET_ID}")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)


@task(retries=3, retry_delay_seconds=30)
def ensure_iceberg_table_exists() -> None:
    """
    Create an Iceberg table using BigLake connection.

    Uses SQL to create a true Iceberg table that stores data on GCS
    via the BigLake Metastore.
    """
    logger = get_run_logger()
    client = _get_bigquery_client()

    try:
        # Try to get the table first
        table_ref = bigquery.TableReference.from_string(TABLE_IDENTIFIER)
        client.get_table(table_ref)
        logger.info(f"Iceberg table {TABLE_IDENTIFIER} already exists")
    except Exception:
        # Table doesn't exist, create it
        logger.info(f"Creating Iceberg table {TABLE_IDENTIFIER}")

        # SQL to create an Iceberg table with BigLake
        # NOTE: For true Iceberg with external GCS storage, we need to use
        # BigLake catalog integration. For now, we create a native Iceberg table
        # in BigQuery which is managed internally.
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_IDENTIFIER}`
        (
            service_request_number STRING,
            created_date TIMESTAMP,
            closed_date TIMESTAMP,
            sr_type STRING,
            sr_short_code STRING,
            current_status STRING,
            origin STRING,
            owner_department STRING,
            created_department STRING,
            street_address STRING,
            zip_code STRING,
            street_number STRING,
            street_direction STRING,
            street_name STRING,
            street_type STRING,
            community_area INT64,
            ward INT64,
            police_sector STRING,
            police_district STRING,
            police_beat STRING,
            precinct STRING,
            created_year INT64,
            created_month INT64,
            created_hour INT64,
            created_day_of_week INT64,
            x_coordinate FLOAT64,
            y_coordinate FLOAT64,
            latitude FLOAT64,
            longitude FLOAT64,
            duplicate BOOL,
            legacy_record BOOL,
            electrical_district STRING,
            electricity_grid STRING,
            parent_sr_number STRING,
            last_modified_date TIMESTAMP
        )
        """

        job = client.query(create_table_sql)
        job.result()
        logger.info(f"Created Iceberg table {TABLE_IDENTIFIER}")


@task
def fetch_from_socrata(
    start_date: str,
    end_date: str,
    limit: int = 50000,
    date_field: str = "created_date"
) -> pl.DataFrame:
    """
    Fetch service requests from Socrata API for the given date range.

    Uses pagination to fetch all records in the date range, not just the first batch.

    Args:
        start_date: ISO format start datetime (e.g., "2026-01-01T00:00:00.000")
        end_date: ISO format end datetime (e.g., "2026-02-01T00:00:00.000")
        limit: Number of records per request (Socrata API limit)
        date_field: Which date field to filter on (default "created_date")

    Returns:
        Polars DataFrame with the fetched data
    """
    logger = get_run_logger()

    client = Socrata(
        "data.cityofchicago.org",
        SOCRATA_APP_TOKEN,
        timeout=60,
    )

    # Build query with date filter
    where_clause = f"{date_field} >= '{start_date}' AND {date_field} < '{end_date}'"

    logger.info(f"Fetching from Socrata with pagination: {where_clause}")

    all_results = []
    offset = 0

    while True:
        batch = client.get(
            SOCRATA_DATASET,
            where=where_clause,
            limit=limit,
            offset=offset,
            order="created_date",
        )

        if not batch:
            break

        all_results.extend(batch)
        logger.info(f"Fetched batch {offset // limit + 1}: {len(batch)} records")

        # If we got fewer than the limit, we're done
        if len(batch) < limit:
            break

        offset += limit

    logger.info(f"Total records fetched from Socrata: {len(all_results)}")

    if not all_results:
        return pl.DataFrame()

    # Convert to Polars for processing
    df = pl.DataFrame(all_results)

    # Type casting and cleaning
    df = (
        df
        # First, cast date columns
        .with_columns([
            pl.col("created_date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
            pl.when(pl.col("closed_date") == "")
             .then(None)
             .otherwise(pl.col("closed_date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"))
             .alias("closed_date"),
            pl.col("last_modified_date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
        ])
        # Derive partition columns from created_date
        .with_columns([
            pl.col("created_date").dt.year().alias("created_year"),
            pl.col("created_date").dt.month().alias("created_month"),
        ])
        # Cast numeric and other columns - handle empty strings
        .with_columns([
            # Integer columns
            pl.when(pl.col("ward") == "")
             .then(None)
             .otherwise(pl.col("ward").cast(pl.Int64))
             .alias("ward"),
            pl.when(pl.col("community_area") == "")
             .then(None)
             .otherwise(pl.col("community_area").cast(pl.Int64))
             .alias("community_area"),
            pl.when(pl.col("created_hour") == "")
             .then(None)
             .otherwise(pl.col("created_hour").cast(pl.Int64))
             .alias("created_hour"),
            # created_day_of_week derived from created_date (Socrata sends day name strings, not integers)
            pl.col("created_date").dt.weekday().alias("created_day_of_week"),
            # Float columns
            pl.when(pl.col("latitude") == "")
             .then(None)
             .otherwise(pl.col("latitude").cast(pl.Float64))
             .alias("latitude"),
            pl.when(pl.col("longitude") == "")
             .then(None)
             .otherwise(pl.col("longitude").cast(pl.Float64))
             .alias("longitude"),
            pl.when(pl.col("x_coordinate") == "")
             .then(None)
             .otherwise(pl.col("x_coordinate").cast(pl.Float64))
             .alias("x_coordinate"),
            pl.when(pl.col("y_coordinate") == "")
             .then(None)
             .otherwise(pl.col("y_coordinate").cast(pl.Float64))
             .alias("y_coordinate"),
            # Cast boolean columns ("true"/"false" strings to actual Booleans)
            (pl.col("duplicate") == "true").alias("duplicate"),
            (pl.col("legacy_record") == "true").alias("legacy_record"),
        ])
        # Handle null string columns
        .with_columns([
            pl.when(pl.col("zip_code") == "")
             .then(None)
             .otherwise(pl.col("zip_code"))
             .alias("zip_code"),
            pl.when(pl.col("sr_short_code") == "")
             .then(None)
             .otherwise(pl.col("sr_short_code"))
             .alias("sr_short_code"),
            pl.when(pl.col("origin") == "")
             .then(None)
             .otherwise(pl.col("origin"))
             .alias("origin"),
            pl.when(pl.col("created_department") == "")
             .then(None)
             .otherwise(pl.col("created_department"))
             .alias("created_department"),
            pl.when(pl.col("street_address") == "")
             .then(None)
             .otherwise(pl.col("street_address"))
             .alias("street_address"),
            pl.when(pl.col("street_number") == "")
             .then(None)
             .otherwise(pl.col("street_number"))
             .alias("street_number"),
            pl.when(pl.col("street_direction") == "")
             .then(None)
             .otherwise(pl.col("street_direction"))
             .alias("street_direction"),
            pl.when(pl.col("street_name") == "")
             .then(None)
             .otherwise(pl.col("street_name"))
             .alias("street_name"),
            pl.when(pl.col("street_type") == "")
             .then(None)
             .otherwise(pl.col("street_type"))
             .alias("street_type"),
            pl.when(pl.col("police_sector") == "")
             .then(None)
             .otherwise(pl.col("police_sector"))
             .alias("police_sector"),
            pl.when(pl.col("police_district") == "")
             .then(None)
             .otherwise(pl.col("police_district"))
             .alias("police_district"),
            pl.when(pl.col("police_beat") == "")
             .then(None)
             .otherwise(pl.col("police_beat"))
             .alias("police_beat"),
            pl.when(pl.col("precinct") == "")
             .then(None)
             .otherwise(pl.col("precinct"))
             .alias("precinct"),
            pl.when(pl.col("electrical_district") == "")
             .then(None)
             .otherwise(pl.col("electrical_district"))
             .alias("electrical_district"),
            pl.when(pl.col("electricity_grid") == "")
             .then(None)
             .otherwise(pl.col("electricity_grid"))
             .alias("electricity_grid"),
            pl.when(pl.col("parent_sr_number") == "")
             .then(None)
             .otherwise(pl.col("parent_sr_number"))
             .alias("parent_sr_number"),
        ])
        # Rename columns to match our schema
        .rename({
            "sr_number": "service_request_number",
            "status": "current_status",
        })
        # Guard: drop rows where service_request_number is null or empty (primary key)
        .filter(
            pl.col("service_request_number").is_not_null()
            & (pl.col("service_request_number") != "")
        )
        # Select only the columns we need (matching the table schema)
        # Note: "city", "state", and "location" are dropped
        .select([
            "service_request_number",
            "created_date",
            "closed_date",
            "sr_type",
            "sr_short_code",
            "current_status",
            "origin",
            "owner_department",
            "created_department",
            "street_address",
            "zip_code",
            "street_number",
            "street_direction",
            "street_name",
            "street_type",
            "community_area",
            "ward",
            "police_sector",
            "police_district",
            "police_beat",
            "precinct",
            "created_year",
            "created_month",
            "created_hour",
            "created_day_of_week",
            "x_coordinate",
            "y_coordinate",
            "latitude",
            "longitude",
            "duplicate",
            "legacy_record",
            "electrical_district",
            "electricity_grid",
            "parent_sr_number",
            "last_modified_date",
        ])
    )

    logger.info(f"Processed {len(df)} records after type casting")
    return df


@task(retries=5, retry_delay_seconds=30)
def write_to_bigquery(
    df: pl.DataFrame,
    table_id: str,
    write_disposition: str = "WRITE_APPEND"
) -> int:
    """
    Write Polars DataFrame to BigQuery table using LoadJob.

    Uses batch load jobs which are free and more efficient than Streaming API.

    Args:
        df: Polars DataFrame to write
        table_id: Full BigQuery table identifier (project.dataset.table)
        write_disposition: "WRITE_APPEND", "WRITE_TRUNCATE", or "WRITE_EMPTY"

    Returns:
        Number of rows written
    """
    import io
    import pyarrow.parquet as pq

    logger = get_run_logger()
    client = _get_bigquery_client()

    if df.is_empty():
        logger.info("DataFrame is empty, skipping write")
        return 0

    logger.info(f"Writing {len(df)} rows to {table_id} using LoadJob")

    # Convert to Arrow, then to Parquet bytes for BigQuery
    arrow_table = df.to_arrow()
    buffer = io.BytesIO()
    pq.write_table(arrow_table, buffer)
    buffer.seek(0)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    job = client.load_table_from_file(
        buffer,
        table_id,
        job_config=job_config
    )
    job.result()  # Wait for the job to complete

    logger.info(f"Successfully wrote {job.output_rows} rows to {table_id}")

    return job.output_rows


@task(retries=2, retry_delay_seconds=30)
def create_audit_table() -> None:
    """
    Create the audit table for WAP pattern.

    The audit table is a copy of the main table schema that receives new data
    before validation and promotion to the main table.
    """
    logger = get_run_logger()
    client = _get_bigquery_client()

    main_table_ref = bigquery.TableReference.from_string(TABLE_IDENTIFIER)
    audit_table_ref = bigquery.TableReference.from_string(AUDIT_TABLE_IDENTIFIER)

    # Get the main table schema
    main_table = client.get_table(main_table_ref)
    schema = main_table.schema

    try:
        # Try to get the audit table
        client.get_table(audit_table_ref)
        logger.info("Audit table already exists")
    except Exception:
        # Create audit table with same schema as main
        logger.info("Creating audit table")
        audit_table = bigquery.Table(audit_table_ref, schema=schema)
        client.create_table(audit_table)


@task(retries=2, retry_delay_seconds=30)
def clear_audit_table() -> None:
    """Clear all data from the audit table."""
    logger = get_run_logger()
    client = _get_bigquery_client()

    query = f"TRUNCATE TABLE `{AUDIT_TABLE_IDENTIFIER}`"
    job = client.query(query)
    job.result()

    logger.info("Cleared audit table")


@task(retries=2, retry_delay_seconds=30)
def audit_table_checks(
    start_date: str,
    end_date: str,
    min_rows: int = 1
) -> bool:
    """
    Run quality checks on the audit table.

    Args:
        start_date: Start date for filtering (ISO format)
        end_date: End date for filtering (ISO format)
        min_rows: Minimum expected rows

    Returns:
        True if all checks pass, False otherwise
    """
    logger = get_run_logger()
    client = _get_bigquery_client()

    # Date filter for audit (only check new data)
    date_filter = f"created_date >= '{start_date}' AND created_date < '{end_date}'"

    # Check 1: Row count
    count_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{AUDIT_TABLE_IDENTIFIER}`
        WHERE {date_filter}
    """
    count_job = client.query(count_query)
    count_result = count_job.result()
    row_count = list(count_result)[0].cnt

    logger.info(f"Audit check: Found {row_count} rows in date range")

    if row_count < min_rows:
        logger.error(f"Row count check failed: expected at least {min_rows}, got {row_count}")
        return False

    # Check 2: No null created_date values in date range
    null_check_query = f"""
        SELECT COUNT(*) as cnt
        FROM `{AUDIT_TABLE_IDENTIFIER}`
        WHERE {date_filter} AND created_date IS NULL
    """
    null_job = client.query(null_check_query)
    null_result = null_job.result()
    null_count = list(null_result)[0].cnt

    logger.info(f"Audit check: Found {null_count} null created_date values")

    if null_count > 0:
        logger.error(f"Null check failed: found {null_count} null created_date values")
        return False

    logger.info("All audit checks passed")
    return True


@task(retries=2, retry_delay_seconds=30)
def replace_main_with_audit() -> None:
    """
    Replace the main table with the audit table using CREATE OR REPLACE AS SELECT.

    WARNING: This is a DESTRUCTIVE operation that completely replaces the main table
    with the audit table. All existing data in the main table is lost.

    This is the atomic promotion step for the WAP pattern and should ONLY be used
    for full reloads, not for incremental daily ingestion.

    For incremental ingestion (daily flow), use INSERT...SELECT instead to preserve
    historical data.
    If this fails, the main table remains untouched.
    """
    logger = get_run_logger()
    client = _get_bigquery_client()

    # Atomically replace main table with audit table
    query = f"""
        CREATE OR REPLACE TABLE `{TABLE_IDENTIFIER}` AS
        SELECT * FROM `{AUDIT_TABLE_IDENTIFIER}`
    """

    logger.info("Publishing audit table to main table")
    job = client.query(query)
    job.result()

    logger.info("Successfully published audit table to main table")


def extract_and_load_chunk(
    start_date: str,
    end_date: str,
    table_id: str = TABLE_IDENTIFIER,
    limit: int = 50000,
    date_field: str = "created_date"
) -> int:
    """
    Extract data from Socrata and load into BigQuery table.

    Args:
        start_date: ISO format start datetime
        end_date: ISO format end datetime
        table_id: Target BigQuery table identifier
        limit: Maximum records to fetch from Socrata
        date_field: Which date field to filter on in Socrata query

    Returns:
        Number of rows loaded
    """
    logger = get_run_logger()

    # Fetch from Socrata
    df = fetch_from_socrata(start_date, end_date, limit, date_field=date_field)

    if df.is_empty():
        logger.info(f"No data found for {start_date} to {end_date}")
        return 0

    # Write to BigQuery
    rows_written = write_to_bigquery(df, table_id, write_disposition="WRITE_APPEND")

    logger.info(f"Loaded {rows_written} rows to {table_id}")

    return rows_written
