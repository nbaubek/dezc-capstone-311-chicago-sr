import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from prefect import flow

from flows.tasks.ingestion import (
    extract_and_load_chunk,
    enable_wap,
    create_audit_branch,
    audit_branch,
    publish_branch,
    cleanup_branch,
    WAP_BRANCH,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TABLE_IDENTIFIER = "chicago_311_lakehouse.chicago_311"


# ---------------------------------------------------------------------------
# Internal helper — shared by yearly and backfill flows
# ---------------------------------------------------------------------------

def _ingest_date_range(start_date: str, end_date: str, chunk_months: int = 1) -> tuple[int, int]:
    """
    Loop over a date range in monthly chunks, calling extract_and_load_chunk
    for each. Used by both yearly_flow and backfill_flow.

    Args:
        start_date:   YYYY-MM-DD (inclusive)
        end_date:     YYYY-MM-DD (exclusive)
        chunk_months: How many months per chunk (default 1)

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
            rows = extract_and_load_chunk(str_start, str_end, TABLE_IDENTIFIER)
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
    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"
    print(f"Starting yearly ingestion for {year}")
    total_rows, failed_chunks = _ingest_date_range(start_date, end_date, chunk_months=1)
    print(f"Yearly ingestion for {year} complete")
    if failed_chunks > 0:
        print(f"⚠️  {failed_chunks} chunk(s) failed - use backfill_flow to retry")


# ---------------------------------------------------------------------------
# Flow 2: Daily ingestion
# Intended for: incremental updates, scheduled daily
# Cadence:      daily via Prefect schedule
# Chunking:     single call — daily delta is small enough
# ---------------------------------------------------------------------------

@flow(name="Daily 311 Ingestion", log_prints=True)
def daily_flow() -> None:
    """
    Ingest new Chicago 311 records using Write-Audit-Publish (WAP).

    1. Write:   Extract last 24 hours of data onto an audit branch.
    2. Audit:   Validate the branch (row count, null checks).
    3. Publish:  Atomically promote branch to main on success.
    4. Cleanup: Remove the audit branch.

    If audit fails, the branch is cleaned up and main is unaffected.
    Scheduled to run daily at midnight.
    """
    start_date = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000")
    end_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")

    print(f"Daily ingestion (WAP): {start_date} → {end_date}")

    # Step 0: Ensure WAP is enabled
    enable_wap(TABLE_IDENTIFIER)

    # Step 1: Create audit branch from current main snapshot
    create_audit_branch(TABLE_IDENTIFIER, WAP_BRANCH)

    # Step 2: Write to audit branch
    rows = extract_and_load_chunk(
        start_date, end_date, TABLE_IDENTIFIER, branch=WAP_BRANCH,
    )

    if not rows:
        print("Daily ingestion complete: no new data")
        cleanup_branch(TABLE_IDENTIFIER, WAP_BRANCH)
        return

    # Step 2: Audit
    passed = audit_branch(TABLE_IDENTIFIER, WAP_BRANCH, start_date, end_date, min_rows=1)

    if not passed:
        print("Audit FAILED — discarding branch, main table unaffected")
        cleanup_branch(TABLE_IDENTIFIER, WAP_BRANCH)
        return

    # Step 3: Publish
    publish_branch(TABLE_IDENTIFIER, WAP_BRANCH)

    # Step 4: Cleanup
    cleanup_branch(TABLE_IDENTIFIER, WAP_BRANCH)

    print(f"Daily ingestion complete: {rows} rows written, audited, and published")


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
    total_rows, failed_chunks = _ingest_date_range(start_date, end_date, chunk_months=chunk_months)
    print("Backfill complete")
    if failed_chunks > 0:
        print(f"⚠️  {failed_chunks} chunk(s) failed - re-run to retry")
