import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from prefect import flow

# Updated imports
from flows.tasks.bigquery_ingestion import (
    audit_table_checks,
    clear_staging_table,
    create_staging_table,
    ensure_dataset_exists,
    ensure_iceberg_table_exists,
    extract_and_load_chunk,
    merge_staging_to_iceberg,
)

load_dotenv()

TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')}.service_requests"
STAGING_TABLE_IDENTIFIER = f"{os.getenv('GCP_PROJECT_ID')}.{os.getenv('BIGQUERY_DATASET_ID', 'chicago_311_lakehouse')}.service_requests_staging"

# ---------------------------------------------------------------------------
# Internal helper — SHARED WAP LOOP
# ---------------------------------------------------------------------------

def _ingest_date_range(
    start_date: str,
    end_date: str,
    chunk_months: int = 1,
    date_field: str = "created_date"
) -> tuple[int, int]:

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    current = start_dt
    chunk = 0
    total_rows = 0
    failed_chunks = 0
    failures = []

    while current < end_dt:
        month = current.month - 1 + chunk_months
        next_year = current.year + month // 12
        next_month = month % 12 + 1
        chunk_end = min(datetime(next_year, next_month, 1), end_dt)

        str_start = current.strftime("%Y-%m-%dT00:00:00.000")
        str_end = chunk_end.strftime("%Y-%m-%dT00:00:00.000")
        chunk += 1

        print(f"Chunk {chunk}: {str_start} → {str_end}")

        try:
            # 1. Extract and Load to Staging (WRITE_TRUNCATE clears the last chunk)
            rows = extract_and_load_chunk(
                str_start, str_end,
                table_id=STAGING_TABLE_IDENTIFIER,
                date_field=date_field,
                write_disposition="WRITE_TRUNCATE"
            )

            if rows:
                # 2. Audit the staging table
                passed = audit_table_checks(str_start, str_end, min_rows=1)

                if passed:
                    # 3. Publish to Iceberg via MERGE
                    merged_rows = merge_staging_to_iceberg()
                    total_rows += merged_rows
                    print(f"  ✓ {merged_rows} rows successfully merged into Iceberg")
                else:
                    raise ValueError("Audit checks failed for chunk")

        except Exception as e:
            failed_chunks += 1
            failures.append((str_start, str_end, str(e)))
            print(f"  ✗ Chunk failed: {e}")

        current = chunk_end

    return total_rows, failed_chunks

# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------

@flow(name="Yearly 311 Ingestion", log_prints=True)
def yearly_flow(year: int) -> None:
    ensure_dataset_exists()
    ensure_iceberg_table_exists()
    create_staging_table() # Ensures staging exists

    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"

    _ingest_date_range(start_date, end_date, chunk_months=1, date_field="created_date")


@flow(name="Daily 311 Ingestion", log_prints=True)
def daily_flow() -> None:
    start_date = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000")
    end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

    ensure_dataset_exists()
    ensure_iceberg_table_exists()
    create_staging_table()

    # Extract & TRUNCATE into Staging
    # NOTE: Daily flow filters on last_modified_date to catch status changes
    rows = extract_and_load_chunk(
        start_date, end_date,
        table_id=STAGING_TABLE_IDENTIFIER,
        date_field="last_modified_date",
        write_disposition="WRITE_TRUNCATE"
    )

    if not rows or rows == 0:
        print("Daily ingestion complete: no new data")
        return

    passed = audit_table_checks(start_date, end_date, min_rows=1)

    if not passed:
        print("Audit FAILED — clearing staging table, main table unaffected")
        clear_staging_table()
        return

    merged_rows = merge_staging_to_iceberg()
    clear_staging_table() # Clean up

    print(f"Daily ingestion complete: {merged_rows} rows merged")


@flow(name="Backfill 311 Pipeline", log_prints=True)
def backfill_flow(start_date: str, end_date: str, chunk_months: int = 1) -> None:
    ensure_dataset_exists()
    ensure_iceberg_table_exists()
    create_staging_table()

    _ingest_date_range(start_date, end_date, chunk_months=chunk_months, date_field="created_date")
