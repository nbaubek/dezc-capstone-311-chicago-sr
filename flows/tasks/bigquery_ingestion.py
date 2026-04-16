"""
BigQuery BigLake ingestion module for Chicago 311 service requests.
Uses a Native BQ Staging table to feed an Iceberg target via MERGE.
"""

import os

import polars as pl
from google.cloud import bigquery
from prefect import get_run_logger, task
from sodapy import Socrata

# Table configuration
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')
TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{DATASET_ID}.service_requests"
# CHANGED: Audit table is now a generic staging table
STAGING_TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{DATASET_ID}.service_requests_staging"

SOCRATA_DATASET = "v6vf-nfxy"
SOCRATA_APP_TOKEN = os.getenv('SOCRATA_APP_TOKEN')

def _get_bigquery_client() -> bigquery.Client:
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

    # SQL to create a true Iceberg table with BigLake
    # WITH CONNECTION links to the BigLake connection created by Terraform
    # table_format='ICEBERG' tells BigQuery to manage this as an Iceberg table on GCS
    # file_format='PARQUET' specifies the storage format
    # storage_uri is the GCS path where Iceberg data will be stored
    # Note: LAKEHOUSE_BUCKET already includes gs:// prefix
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
        WITH CONNECTION `{os.getenv('GCP_PROJECT_ID')}.us.biglake-connection`
        OPTIONS (
            table_format='ICEBERG',
            file_format='PARQUET',
            storage_uri='{os.getenv("LAKEHOUSE_BUCKET")}/{DATASET_ID}/service_requests'
        )
    """

    try:
        job = client.query(create_table_sql)
        job.result()
        logger.info(f"Iceberg table {TABLE_IDENTIFIER} is ready")
    except Exception as e:
        # If table already exists or connection issue, log but don't fail
        logger.info(f"Table creation note: {e}")

# --- THE BIG CHANGES BELOW ---

@task(retries=2, retry_delay_seconds=30)
def create_staging_table() -> None:
    """
    Create the staging table for WAP pattern.
    CRITICAL: This is a NATIVE BigQuery table, not Iceberg. This allows us to
    use WRITE_TRUNCATE efficiently before merging into the main Iceberg table.
    """
    logger = get_run_logger()
    client = _get_bigquery_client()

    # Standard BQ table creation. No WITH CONNECTION or ICEBERG options.
    create_staging_sql = f"""
    CREATE TABLE IF NOT EXISTS `{STAGING_TABLE_IDENTIFIER}`
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
    try:
        job = client.query(create_staging_sql)
        job.result()
        logger.info(f"Native staging table {STAGING_TABLE_IDENTIFIER} is ready")
    except Exception as e:
        logger.info(f"Staging table creation note: {e}")

@task(retries=2, retry_delay_seconds=30)
def clear_staging_table() -> None:
    """Clear all data from the native staging table."""
    logger = get_run_logger()
    client = _get_bigquery_client()
    query = f"TRUNCATE TABLE `{STAGING_TABLE_IDENTIFIER}`"
    job = client.query(query)
    job.result()
    logger.info("Cleared native staging table")

@task
def fetch_from_socrata(
    start_date: str,
    end_date: str,
    limit: int = 25000,
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
        timeout=120,
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

    # Columns that may not exist in all Socrata responses
    optional_cols = ["electrical_district", "electricity_grid", "parent_sr_number"]
    existing_optional = [c for c in optional_cols if c in df.columns]

    # Helper to safely handle empty string columns
    def safe_empty_to_null(col_name):
        """Convert empty strings to null, preserving non-empty values."""
        if col_name in df.columns:
            return pl.when(pl.col(col_name) == "").then(None).otherwise(pl.col(col_name)).alias(col_name)
        return None

    def safe_cast_int(col_name):
        """Handle empty strings before casting to Int64."""
        if col_name in df.columns:
            return pl.when(pl.col(col_name) == "").then(None).otherwise(pl.col(col_name).cast(pl.Int64)).alias(col_name)
        return None

    def safe_cast_float(col_name):
        """Handle empty strings before casting to Float64."""
        if col_name in df.columns:
            return pl.when(pl.col(col_name) == "").then(None).otherwise(pl.col(col_name).cast(pl.Float64)).alias(col_name)
        return None

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
            pl.col("created_date").dt.weekday().alias("created_day_of_week"),
        ])
        # Cast numeric columns - handle empty strings
        .with_columns([
            safe_cast_int("ward"),
            safe_cast_int("community_area"),
            safe_cast_int("created_hour"),
            safe_cast_float("latitude"),
            safe_cast_float("longitude"),
            safe_cast_float("x_coordinate"),
            safe_cast_float("y_coordinate"),
            # Boolean columns ("true"/"false" strings to actual Booleans)
            (pl.col("duplicate") == "true").alias("duplicate"),
            (pl.col("legacy_record") == "true").alias("legacy_record"),
        ])
        # Handle optional columns that may not exist in all responses
        .with_columns([
            safe_empty_to_null("zip_code"),
            safe_empty_to_null("sr_short_code"),
            safe_empty_to_null("origin"),
            safe_empty_to_null("created_department"),
            safe_empty_to_null("street_address"),
            safe_empty_to_null("street_number"),
            safe_empty_to_null("street_direction"),
            safe_empty_to_null("street_name"),
            safe_empty_to_null("street_type"),
            safe_empty_to_null("police_sector"),
            safe_empty_to_null("police_district"),
            safe_empty_to_null("police_beat"),
            safe_empty_to_null("precinct"),
        ])
        # Handle optional columns - only if they exist
        .with_columns([col for col in [safe_empty_to_null(c) for c in existing_optional] if col is not None])
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
    )

    # STRICT SCHEMA ENFORCEMENT
    # Socrata omits columns if they are null for an entire batch.
    # We must explicitly inject them as nulls to prevent BigQuery WRITE_TRUNCATE from dropping them.
    expected_schema = {
        "service_request_number": pl.String,
        "created_date": pl.Datetime,
        "closed_date": pl.Datetime,
        "sr_type": pl.String,
        "sr_short_code": pl.String,
        "current_status": pl.String,
        "origin": pl.String,
        "owner_department": pl.String,
        "created_department": pl.String,
        "street_address": pl.String,
        "zip_code": pl.String,
        "street_number": pl.String,
        "street_direction": pl.String,
        "street_name": pl.String,
        "street_type": pl.String,
        "community_area": pl.Int64,
        "ward": pl.Int64,
        "police_sector": pl.String,
        "police_district": pl.String,
        "police_beat": pl.String,
        "precinct": pl.String,
        "created_year": pl.Int64,
        "created_month": pl.Int64,
        "created_hour": pl.Int64,
        "created_day_of_week": pl.Int64,
        "x_coordinate": pl.Float64,
        "y_coordinate": pl.Float64,
        "latitude": pl.Float64,
        "longitude": pl.Float64,
        "duplicate": pl.Boolean,
        "legacy_record": pl.Boolean,
        "electrical_district": pl.String,
        "electricity_grid": pl.String,
        "parent_sr_number": pl.String,
        "last_modified_date": pl.Datetime,
    }

    # Inject missing columns as strongly-typed nulls
    for col_name, col_dtype in expected_schema.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_dtype).alias(col_name))

    # Strip hidden whitespace from primary keys just in case Socrata pads them
    df = df.with_columns(pl.col("service_request_number").str.strip_chars())

    # Force exact column order to match BigQuery perfectly
    df = df.select(list(expected_schema.keys()))

    logger.info(f"Processed {len(df)} records with strict schema enforcement")
    return df

@task(retries=5, retry_delay_seconds=30)
def write_to_bigquery(
    df: pl.DataFrame,
    table_id: str,
    write_disposition: str = "WRITE_TRUNCATE" # CHANGED DEFAULT TO TRUNCATE
) -> int:
    """Write Polars DataFrame to BigQuery table using LoadJob."""
    import io

    import pyarrow.parquet as pq

    logger = get_run_logger()
    client = _get_bigquery_client()

    if df.is_empty():
        return 0

    arrow_table = df.to_arrow()
    buffer = io.BytesIO()
    pq.write_table(arrow_table, buffer)
    buffer.seek(0)

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    job = client.load_table_from_file(buffer, table_id, job_config=job_config)
    job.result()
    output_rows = job.output_rows
    if output_rows is None:
        msg = f"BigQuery load job returned None rows for {table_id}"
        raise ValueError(msg)
    logger.info(f"Successfully wrote {output_rows} rows to {table_id} via {write_disposition}")
    return output_rows  # type: ignore[no-any-return]

@task(retries=2, retry_delay_seconds=30)
def audit_table_checks(start_date: str, end_date: str, min_rows: int = 1) -> bool:
    """Run quality checks on the staging table."""
    logger = get_run_logger()
    client = _get_bigquery_client()

    # Because we truncate the staging table every chunk, we don't even need the date_filter
    # anymore for the audit. We just audit whatever is currently in the staging table.
    count_query = f"SELECT COUNT(*) as cnt FROM `{STAGING_TABLE_IDENTIFIER}`"
    row_count = next(iter(client.query(count_query).result())).cnt

    if row_count < min_rows:
        logger.error(f"Audit failed: expected {min_rows}, got {row_count}")
        return False

    null_check_query = f"SELECT COUNT(*) as cnt FROM `{STAGING_TABLE_IDENTIFIER}` WHERE created_date IS NULL"
    null_count = next(iter(client.query(null_check_query).result())).cnt

    if null_count > 0:
        logger.error(f"Audit failed: {null_count} null created_dates")
        return False

    return True

@task(retries=2, retry_delay_seconds=30)
def merge_staging_to_iceberg() -> int:
    """Executes the WAP Publish step. Safe for both daily and historical loads."""
    logger = get_run_logger()
    client = _get_bigquery_client()

    merge_query = f"""
        MERGE INTO `{TABLE_IDENTIFIER}` AS target
        USING (
            -- STRICT DEDUPLICATION:
            -- If Socrata gives us the same ticket 3 times in one chunk,
            -- only take the one with the most recent last_modified_date
            SELECT * EXCEPT(row_num)
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY service_request_number
                        ORDER BY last_modified_date DESC
                    ) as row_num
                FROM `{STAGING_TABLE_IDENTIFIER}`
            )
            WHERE row_num = 1
        ) AS source
        ON target.service_request_number = source.service_request_number

        -- 1. UPDATE EXISTING RECORDS
        WHEN MATCHED AND source.last_modified_date > target.last_modified_date THEN
            UPDATE SET
                target.current_status = source.current_status,
                target.closed_date = source.closed_date,
                target.last_modified_date = source.last_modified_date,
                target.duplicate = source.duplicate,
                target.parent_sr_number = source.parent_sr_number,
                target.owner_department = source.owner_department

        -- 2. INSERT BRAND NEW RECORDS
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                service_request_number, sr_type, sr_short_code, current_status, owner_department,
                origin, created_date, last_modified_date, closed_date, street_address, zip_code,
                street_number, street_direction, street_name, street_type, community_area, ward,
                police_sector, police_district, police_beat, precinct, latitude, longitude,
                x_coordinate, y_coordinate, duplicate, legacy_record, created_year, created_month,
                created_hour, created_day_of_week, created_department, electrical_district,
                electricity_grid, parent_sr_number
            )
            VALUES (
                source.service_request_number, source.sr_type, source.sr_short_code, source.current_status,
                source.owner_department, source.origin, source.created_date, source.last_modified_date,
                source.closed_date, source.street_address, source.zip_code, source.street_number,
                source.street_direction, source.street_name, source.street_type, source.community_area,
                source.ward, source.police_sector, source.police_district, source.police_beat,
                source.precinct, source.latitude, source.longitude, source.x_coordinate, source.y_coordinate,
                source.duplicate, source.legacy_record, source.created_year, source.created_month,
                source.created_hour, source.created_day_of_week, source.created_department,
                source.electrical_district, source.electricity_grid, source.parent_sr_number
            )
    """
    job = client.query(merge_query)
    job.result()
    merged_rows = job.num_dml_affected_rows
    if merged_rows is None:
        msg = "BigQuery merge job returned None for num_dml_affected_rows"
        raise ValueError(msg)
    logger.info(f"Successfully merged {merged_rows} rows into Iceberg table")
    return merged_rows

def extract_and_load_chunk(
    start_date: str,
    end_date: str,
    table_id: str = STAGING_TABLE_IDENTIFIER,
    limit: int = 50000,
    date_field: str = "created_date",
    write_disposition: str = "WRITE_TRUNCATE" # Pass this through
) -> int:
    """Extract from Socrata, load to BigQuery."""
    get_run_logger()

    # Needs to be imported here if you define fetch_from_socrata above
    df = fetch_from_socrata(start_date, end_date, limit, date_field=date_field)
    if df.is_empty():
        return 0

    rows_written = write_to_bigquery(df, table_id, write_disposition=write_disposition)
    return rows_written
