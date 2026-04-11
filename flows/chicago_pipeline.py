import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from prefect import flow

# BigQuery ingestion functions
from flows.tasks.bigquery_ingestion import (
    extract_and_load_chunk,
    ensure_dataset_exists,
    ensure_iceberg_table_exists,
    create_audit_table,
    clear_audit_table,
    audit_table_checks,
    _get_bigquery_client,
    # replace_main_with_audit is NOT used in daily_flow (incremental INSERT)
    # It's available for full reload scenarios where you want to replace
    # the entire main table with the audit table.
)

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')}.service_requests"
AUDIT_TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')}.service_requests_audit"


# ---------------------------------------------------------------------------
# Internal helper — shared by yearly and backfill flows
# ---------------------------------------------------------------------------

def _ingest_date_range(
    start_date: str,
    end_date: str,
    table_id: str,
    chunk_months: int = 1,
    date_field: str = "created_date"
) -> tuple[int, int]:
    """
    Loop over a date range in monthly chunks, calling extract_and_load_chunk
    for each. Used by both yearly_flow and backfill_flow.

    Args:
        start_date:   YYYY-MM-DD (inclusive)
        end_date:     YYYY-MM-DD (exclusive)
        table_id:     BigQuery table identifier to load into
        chunk_months: How many months per chunk (default 1)
        date_field:   Which date field to filter on (default "created_date")

    Returns:
        Tuple of (total_rows, failed_chunks)
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    current = start_dt
    chunk = 0
    total_rows = 0
    failed_chunks = 0
    failures = []

    while current < end_dt:
        # Advance by chunk_months — simple month arithmetic
        month = current.month - 1 + chunk_months
        next_year = current.year + month // 12
        next_month = month % 12 + 1
        chunk_end = min(
            datetime(next_year, next_month, 1),
            end_dt,
        )

        str_start = current.strftime("%Y-%m-%dT00:00:00.000")
        str_end = chunk_end.strftime("%Y-%m-%dT00:00:00.000")
        chunk += 1

        print(f"Chunk {chunk}: {str_start} → {str_end}")

        try:
            rows = extract_and_load_chunk(str_start, str_end, table_id, date_field=date_field)
            if rows:
                total_rows += rows
                print(f"  ✓ {rows} rows loaded")
        except Exception as e:
            # Log and continue — one bad chunk doesn't abort the whole range.
            failed_chunks += 1
            failures.append((str_start, str_end, str(e)))
            print(f"  ✗ Chunk failed: {e}")

        current = chunk_end

    print(f"Range {start_date} → {end_date} complete ({chunk} chunks)")
    print(f"  Total rows: {total_rows:,}")
    if failed_chunks > 0:
        print(f"  Failed chunks: {failed_chunks}")
        for start, end, error in failures:
            print(f"    - {start} → {end}: {error}")

    return total_rows, failed_chunks


# ---------------------------------------------------------------------------
# Flow 1: Yearly ingestion
# Intended for: initial load of 2024, 2025, 2026
# Cadence:      manual / once per year
# Chunking:     monthly (balances Socrata reliability vs. Prefect run count)
# ---------------------------------------------------------------------------

@flow(name="Yearly 311 Ingestion", log_prints=True)
def yearly_flow(year: int) -> None:
    """
    Ingest one full calendar year of Chicago 311 data in monthly chunks.

    Run once per year for the initial load. For partial years (e.g. 2026
    mid-year), end_date naturally stops at Jan 1 of the following year and
    Socrata returns only what exists.

    Args:
        year: Calendar year to ingest e.g. 2024
    """
    print(f"Starting yearly ingestion for {year}")

    # Ensure BigQuery dataset and Iceberg table exist
    ensure_dataset_exists()
    ensure_iceberg_table_exists()

    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"
    total_rows, failed_chunks = _ingest_date_range(
        start_date, end_date,
        table_id=TABLE_IDENTIFIER,
        chunk_months=1,
        date_field="created_date"
    )
    print(f"Yearly ingestion for {year} complete")
    if failed_chunks > 0:
        print(f"⚠️  {failed_chunks} chunk(s) failed - use backfill_flow to retry")


# ---------------------------------------------------------------------------
# Flow 2: Daily ingestion (WAP via audit table swap)
# Intended for: incremental updates, scheduled daily
# Cadence:      daily via Prefect schedule
# Pattern:      Audit table swap for atomic WAP
# ---------------------------------------------------------------------------

@flow(name="Daily 311 Ingestion", log_prints=True)
def daily_flow() -> None:
    """
    Ingest new Chicago 311 records using Write-Audit-Publish (WAP).

    BigQuery doesn't support Iceberg's native branching, so we use
    audit table swap pattern:

    1. Setup:   Ensure dataset, main table, and audit table exist.
    2. Clear:   Clear the audit table.
    3. Write:   Extract last 24 hours and write to audit table.
    4. Audit:   Validate the audit table (row count, null checks).
    5. Merge:   Insert audit data into main table.
    6. Cleanup: Clear audit table for next run.

    If audit fails, the audit table is cleared and main is unaffected.
    Scheduled to run daily at midnight.
    """
    start_date = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000")
    end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

    print(f"Daily ingestion (WAP): {start_date} → {end_date}")

    # Step 0: Ensure BigQuery dataset and tables exist
    ensure_dataset_exists()
    ensure_iceberg_table_exists()
    create_audit_table()

    # Step 1: Clear audit table
    clear_audit_table()

    # Step 2: Write to audit table
    rows = extract_and_load_chunk(
        start_date, end_date, AUDIT_TABLE_IDENTIFIER,
    )

    if not rows or rows == 0:
        print("Daily ingestion complete: no new data")
        return

    # Step 3: Audit
    passed = audit_table_checks(start_date, end_date, min_rows=1)

    if not passed:
        print("Audit FAILED — clearing audit table, main table unaffected")
        clear_audit_table()
        return

    # Step 4: Publish - Insert audit data into main table
    # Since we're doing incremental loads, we need to insert only new records
    # to avoid duplicates. We'll use a merge-like approach with INSERT...SELECT
    # that excludes records already in the main table.
    client = _get_bigquery_client()

    merge_query = f"""
        INSERT INTO `{TABLE_IDENTIFIER}`
        SELECT audit.*
        FROM `{AUDIT_TABLE_IDENTIFIER}` audit
        LEFT JOIN `{TABLE_IDENTIFIER}` main
            ON audit.service_request_number = main.service_request_number
        WHERE main.service_request_number IS NULL
    """

    print(f"Merging {rows} rows from audit to main table")
    merge_job = client.query(merge_query)
    merge_job.result()
    merged_rows = merge_job.num_dml_affected_rows
    print(f"Merged {merged_rows} new rows into main table")

    # Step 5: Cleanup - Clear audit table for next run
    clear_audit_table()

    print(f"Daily ingestion complete: {merged_rows} new rows merged")


# ---------------------------------------------------------------------------
# Flow 3: Backfill
# Intended for: re-ingesting arbitrary ranges, fixing gaps, corrections
# Cadence:      manual
# Chunking:     monthly by default, configurable
# ---------------------------------------------------------------------------

@flow(name="Backfill 311 Pipeline", log_prints=True)
def backfill_flow(
    start_date: str,
    end_date: str,
    chunk_months: int = 1,
) -> None:
    """
    Re-ingest Chicago 311 data for an arbitrary date range.

    Use this to:
    - Fill gaps caused by failed daily runs
    - Re-ingest after schema corrections
    - Ingest a specific month or quarter on demand

    Args:
        start_date:   Start date YYYY-MM-DD (inclusive)
        end_date:     End date   YYYY-MM-DD (exclusive)
        chunk_months: Months per chunk, default 1.
                      Use smaller values for targeted fixes,
                      larger values for wide historical ranges.

    Examples:
        backfill_flow("2024-03-01", "2024-04-01")            # one month
        backfill_flow("2024-01-01", "2025-01-01", chunk_months=3)  # quarterly chunks
    """
    print(f"Starting backfill: {start_date} → {end_date} ({chunk_months}-month chunks)")

    # Ensure BigQuery dataset and Iceberg table exist
    ensure_dataset_exists()
    ensure_iceberg_table_exists()

    total_rows, failed_chunks = _ingest_date_range(
        start_date, end_date,
        table_id=TABLE_IDENTIFIER,
        chunk_months=chunk_months,
        date_field="created_date"
    )
    print("Backfill complete")
    if failed_chunks > 0:
        print(f"⚠️  {failed_chunks} chunk(s) failed - re-run to retry")
