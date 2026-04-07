import os
import time
import polars as pl
from sodapy import Socrata
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform, MonthTransform, IdentityTransform
from pyiceberg.types import (
    NestedField, LongType, StringType, BooleanType,
    TimestampType, DoubleType, IntegerType
)
from prefect import task, get_run_logger
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASET_ID = "v6vf-nfxy"
SOCRATA_HOST = "data.cityofchicago.org"
SOCRATA_TIMEOUT = 120


# ---------------------------------------------------------------------------
# Type casting
# ---------------------------------------------------------------------------

def _get_strict_type_casts() -> list:
    """Return Polars type cast expressions for the 311 dataset."""
    return [
        pl.col("created_date").str.to_datetime(strict=False),
        pl.col("last_modified_date").str.to_datetime(strict=False),
        pl.col("closed_date").str.to_datetime(strict=False),
        pl.col("duplicate").cast(pl.Boolean, strict=False),
        pl.col("legacy_record").cast(pl.Boolean, strict=False),
        pl.col("sr_number").cast(pl.Int64, strict=False),
        pl.col("x_coordinate").cast(pl.Float64, strict=False),
        pl.col("y_coordinate").cast(pl.Float64, strict=False),
        pl.col("latitude").cast(pl.Float64, strict=False),
        pl.col("longitude").cast(pl.Float64, strict=False),
        pl.col("community_area").cast(pl.Int32, strict=False),
        pl.col("ward").cast(pl.Int32, strict=False),
        pl.col("created_hour").cast(pl.Int8, strict=False),
        pl.col("created_day_of_week").cast(pl.Int8, strict=False),
        pl.col("created_month").cast(pl.Int8, strict=False),
    ]


def _add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add derived columns from the timestamp fields."""
    # Create created_year from created_date
    return df.with_columns(
        pl.col("created_date").dt.year().alias("created_year")
    )


def _process_page_to_df(page: list) -> pl.DataFrame:
    """Convert a raw Socrata page to a typed Polars DataFrame."""
    return (
        pl.from_dicts(page, infer_schema_length=10000)
        .with_columns(_get_strict_type_casts())
        .pipe(_add_derived_columns)
        .drop(["city", "district", "location"], strict=False)
    )


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------

def _get_catalog():
    """Return the SQL catalog instance from environment variables."""
    return load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": os.getenv("CATALOG_URI"),
            "warehouse": os.getenv("LAKEHOUSE_BUCKET"),
        }
    )


def _arrow_type_to_iceberg(field_type_str: str):
    """Map a PyArrow type string to the corresponding PyIceberg type."""
    if "int64" in field_type_str:
        return LongType()
    if any(t in field_type_str for t in ["int32", "int16", "int8"]):
        return IntegerType()
    if any(t in field_type_str for t in ["double", "float64"]):
        return DoubleType()
    if "bool" in field_type_str:
        return BooleanType()
    if "timestamp" in field_type_str or "datetime" in field_type_str:
        return TimestampType()
    return StringType()


def _build_iceberg_schema(arrow_schema) -> Schema:
    """Convert a PyArrow schema to a PyIceberg Schema."""
    fields = [
        NestedField(
            field_id=i + 1,
            name=field.name,
            field_type=_arrow_type_to_iceberg(str(field.type)),
            required=False,
        )
        for i, field in enumerate(arrow_schema)
    ]
    return Schema(*fields)


def _get_or_create_table(catalog, table_identifier: str, arrow_schema):
    """
    Load an existing Iceberg table or create it partitioned by year and month.

    Partition strategy: year(created_date) / month(created_date)
    This gives optimal predicate pushdown for both yearly and daily queries.

    Args:
        catalog: PyIceberg catalog instance
        table_identifier: Fully qualified name e.g. "chicago_311.service_requests"
        arrow_schema: PyArrow schema from the first ingested chunk

    Returns:
        PyIceberg Table instance
    """
    try:
        return catalog.load_table(table_identifier)
    except NoSuchTableError:
        namespace = table_identifier.split(".")[0]
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass

        iceberg_schema = _build_iceberg_schema(arrow_schema)

        created_year_field = next(
            (f for f in iceberg_schema.fields if f.name == "created_year"), None
        )
        created_month_field = next(
            (f for f in iceberg_schema.fields if f.name == "created_month"), None
        )

        if not created_year_field:
            raise ValueError("Field 'created_year' not found in schema")
        if not created_month_field:
            raise ValueError("Field 'created_month' not found in schema")

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=created_year_field.field_id,
                field_id=1000,
                transform=IdentityTransform(),
                name="year",
            ),
            PartitionField(
                source_id=created_month_field.field_id,
                field_id=1001,
                transform=IdentityTransform(),
                name="month",
            ),
        )

        return catalog.create_table(
            table_identifier,
            schema=iceberg_schema,
            partition_spec=partition_spec,
        )


# ---------------------------------------------------------------------------
# Extraction generator
# ---------------------------------------------------------------------------

def _extract_311_data(start_date: str, end_date: str, chunk_size: int = 50000):
    """
    Yield typed Polars DataFrames from the Chicago 311 Socrata API.

    Paginates with offset + deterministic ordering. Each chunk retries
    independently so a transient failure doesn't restart the full range.

    Args:
        start_date: Socrata format YYYY-MM-DDTHH:MM:SS.000 (inclusive)
        end_date:   Socrata format YYYY-MM-DDTHH:MM:SS.000 (exclusive)
        chunk_size: Records per API request (default 50000)

    Yields:
        Polars DataFrame per chunk
    """
    app_token = os.getenv("SOCRATA_APP_TOKEN")
    if not app_token:
        raise ValueError("SOCRATA_APP_TOKEN environment variable is not set")

    client = Socrata(SOCRATA_HOST, app_token, timeout=SOCRATA_TIMEOUT)
    query = f"created_date >= '{start_date}' AND created_date < '{end_date}'"
    offset = 0
    total_records = 0
    max_retries = 3

    while True:
        retry_count = 0
        while retry_count < max_retries:
            try:
                page = client.get(
                    DATASET_ID,
                    where=query,
                    limit=chunk_size,
                    offset=offset,
                    order="created_date ASC, sr_number ASC",
                )

                if not page:
                    print(f"Extracted {total_records} total records ({start_date} → {end_date})")
                    return

                df = _process_page_to_df(page)
                total_records += df.height
                yield df

                if len(page) < chunk_size:
                    print(f"Extracted {total_records} total records ({start_date} → {end_date})")
                    return

                offset += chunk_size
                print(f"  {total_records} records so far...")
                time.sleep(0.5)
                break

            except Exception as e:
                retry_count += 1
                print(f"  Chunk at offset {offset} failed (attempt {retry_count}/{max_retries}): {e}")
                if retry_count >= max_retries:
                    raise RuntimeError(
                        f"Max retries reached at offset {offset} "
                        f"for range {start_date} → {end_date}"
                    ) from e
                time.sleep(2 ** retry_count)


# ---------------------------------------------------------------------------
# Prefect task — the single unit of work used by all flows
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=60)
def extract_and_load_chunk(
    start_date: str,
    end_date: str,
    table_identifier: str,
    chunk_size: int = 50000,
    branch: str | None = None,
) -> int:
    """
    Extract 311 data for a date range and append it to an Iceberg table.

    Memory-efficient: each Socrata page is typed and appended immediately.
    The catalog and table handle are initialized once and reused.

    Args:
        start_date:       Socrata format YYYY-MM-DDTHH:MM:SS.000 (inclusive)
        end_date:         Socrata format YYYY-MM-DDTHH:MM:SS.000 (exclusive)
        table_identifier: Iceberg table e.g. "chicago_311.service_requests"
        chunk_size:       Records per Socrata request (default 50000)
        branch:           Iceberg branch name for WAP. None writes to main.

    Returns:
        Total rows appended
    """
    logger = get_run_logger()
    catalog = _get_catalog()
    table = None
    total_rows = 0
    max_retries = 5
    retry_delay = 30

    for chunk_df in _extract_311_data(start_date, end_date, chunk_size):
        if chunk_df.is_empty():
            continue

        arrow_table = chunk_df.to_arrow()

        # Initialize table on first non-empty chunk
        if table is None:
            table = _get_or_create_table(catalog, table_identifier, arrow_table.schema)
            logger.info(f"Using table: {table_identifier}")

        # Retry the append operation with backoff
        retry_count = 0
        while retry_count < max_retries:
            try:
                if branch:
                    table.append(arrow_table, branch=branch)
                else:
                    table.append(arrow_table)
                total_rows += arrow_table.num_rows
                logger.info(f"Appended {arrow_table.num_rows} rows (running total: {total_rows})")
                break  # Success, exit retry loop
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    raise RuntimeError(
                        f"Failed to append chunk after {max_retries} retries: {e}"
                    ) from e

                # Log retry and reload table handle
                logger.warning(
                    f"Append failed (attempt {retry_count}/{max_retries}): {e}. "
                    f"Reloading table and retrying in {retry_delay}s..."
                )
                import time
                time.sleep(retry_delay)

                # Reload table to get fresh metadata
                table = catalog.load_table(table_identifier)

    logger.info(f"Completed {start_date} → {end_date}: {total_rows} rows")
    return total_rows


# ---------------------------------------------------------------------------
# WAP (Write-Audit-Publish) helpers
# ---------------------------------------------------------------------------

WAP_BRANCH = "daily-audit"


@task(retries=2, retry_delay_seconds=30)
def enable_wap(table_identifier: str) -> None:
    """Enable write-audit-publish on the table if not already enabled."""
    logger = get_run_logger()
    catalog = _get_catalog()
    table = catalog.load_table(table_identifier)

    if table.properties.get("write.wap.enabled") != "true":
        with table.transaction() as txn:
            txn.set_properties({"write.wap.enabled": "true"})
        logger.info("WAP enabled on table")
    else:
        logger.info("WAP already enabled on table")


@task(retries=2, retry_delay_seconds=30)
def create_audit_branch(table_identifier: str, branch: str) -> None:
    """Create (or recreate) the audit branch from the current main snapshot."""
    logger = get_run_logger()
    catalog = _get_catalog()
    table = catalog.load_table(table_identifier)

    # Remove stale branch if it exists from a previous failed run
    if branch in table.refs():
        table.manage_snapshots().remove_branch(branch).commit()
        table = catalog.load_table(table_identifier)
        logger.info(f"Removed stale branch '{branch}'")

    snapshot_id = table.current_snapshot().snapshot_id
    table.manage_snapshots().create_branch(snapshot_id, branch).commit()
    logger.info(f"Created branch '{branch}' from snapshot {snapshot_id}")


@task(retries=2, retry_delay_seconds=30)
def audit_branch(
    table_identifier: str,
    branch: str,
    start_date: str,
    end_date: str,
    min_rows: int = 1,
) -> bool:
    """
    Run quality checks on the WAP audit branch.

    Only scans the data within the date range, not the entire branch.

    Returns True if the branch passes all checks.
    """
    logger = get_run_logger()
    catalog = _get_catalog()
    table = catalog.load_table(table_identifier)

    try:
        # Get the branch's snapshot ID, then scan by snapshot_id with date filter
        branch_snapshot = table.snapshot_by_name(branch)
        # Filter by date range to only audit new data
        date_filter = f"created_date >= '{start_date}' AND created_date < '{end_date}'"
        arrow_table = table.scan(
            snapshot_id=branch_snapshot.snapshot_id,
            row_filter=date_filter,
        ).to_arrow()
    except Exception as e:
        logger.error(f"Failed to read branch '{branch}': {e}")
        return False

    row_count = arrow_table.num_rows
    logger.info(f"Branch '{branch}' contains {row_count} rows in date range")

    if row_count < min_rows:
        logger.error(f"Audit failed: {row_count} rows < minimum {min_rows}")
        return False

    # Check for null created_date
    df = pl.from_arrow(arrow_table)
    null_dates = df.filter(pl.col("created_date").is_null()).height
    if null_dates > 0:
        logger.error(f"Audit failed: {null_dates} rows have null created_date")
        return False

    logger.info("Audit passed")
    return True


@task(retries=2, retry_delay_seconds=30)
def publish_branch(table_identifier: str, branch: str) -> None:
    """Publish the audit branch to main via set_current_snapshot."""
    logger = get_run_logger()
    catalog = _get_catalog()
    table = catalog.load_table(table_identifier)

    table.manage_snapshots().set_current_snapshot(ref_name=branch).commit()
    logger.info(f"Published branch '{branch}' to main")


@task(retries=2, retry_delay_seconds=30)
def cleanup_branch(table_identifier: str, branch: str) -> None:
    """Remove the audit branch after publish."""
    logger = get_run_logger()
    catalog = _get_catalog()
    table = catalog.load_table(table_identifier)

    try:
        table.manage_snapshots().remove_branch(branch).commit()
        logger.info(f"Removed branch '{branch}'")
    except Exception as e:
        logger.warning(f"Could not remove branch '{branch}': {e}")
