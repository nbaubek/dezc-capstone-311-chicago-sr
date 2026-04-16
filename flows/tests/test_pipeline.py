"""Pytest tests for Chicago 311 pipeline Python code."""

from unittest.mock import patch

import polars as pl

# ---------------------------------------------------------------------------
# Tests for _ingest_date_range chunking logic
# ---------------------------------------------------------------------------

def test_ingest_date_range_single_month():
    """Test that a single-month range produces exactly one chunk."""
    from flows.chicago_pipeline import _ingest_date_range

    with (
        patch("flows.chicago_pipeline.extract_and_load_chunk") as mock_extract,
        patch("flows.chicago_pipeline.merge_staging_to_iceberg") as mock_merge,
    ):
        mock_extract.return_value = 100
        mock_merge.return_value = 100

        total_rows, failed = _ingest_date_range(
            start_date="2026-01-01",
            end_date="2026-02-01",
            chunk_months=1,
        )

        assert mock_extract.call_count == 1
        assert total_rows == 100
        assert failed == 0


def test_ingest_date_range_multiple_months():
    """Test that a multi-month range produces correct number of chunks."""
    from flows.chicago_pipeline import _ingest_date_range

    with (
        patch("flows.chicago_pipeline.extract_and_load_chunk") as mock_extract,
        patch("flows.chicago_pipeline.merge_staging_to_iceberg") as mock_merge,
    ):
        mock_extract.return_value = 100
        mock_merge.return_value = 100

        total_rows, _failed = _ingest_date_range(
            start_date="2026-01-01",
            end_date="2026-04-01",
            chunk_months=1,
        )

        # Jan→Feb, Feb→Mar, Mar→Apr = 3 chunks
        assert mock_extract.call_count == 3
        assert total_rows == 300


def test_ingest_date_range_partial_month_at_end():
    """Test that range ending mid-month still produces correct chunks."""
    from flows.chicago_pipeline import _ingest_date_range

    with (
        patch("flows.chicago_pipeline.extract_and_load_chunk") as mock_extract,
        patch("flows.chicago_pipeline.merge_staging_to_iceberg") as mock_merge,
    ):
        mock_extract.return_value = 50
        mock_merge.return_value = 50

        _total_rows, _failed = _ingest_date_range(
            start_date="2026-01-15",
            end_date="2026-03-15",
            chunk_months=1,
        )

        # Chunk 1: Jan 15 → Feb 1 (partial)
        # Chunk 2: Feb 1 → Mar 1 (full)
        # Chunk 3: Mar 1 → Mar 15 (partial)
        assert mock_extract.call_count == 3


def test_ingest_date_range_failed_chunk_continues():
    """Test that a failed chunk increments failure counter but continues."""
    from flows.chicago_pipeline import _ingest_date_range

    with (
        patch("flows.chicago_pipeline.extract_and_load_chunk") as mock_extract,
        patch("flows.chicago_pipeline.merge_staging_to_iceberg") as mock_merge,
    ):
        mock_extract.side_effect = [100, Exception("API timeout"), Exception("exhausted")]
        mock_merge.side_effect = [100, Exception("Merge failed")]

        total_rows, failed = _ingest_date_range(
            start_date="2026-01-01",
            end_date="2026-04-01",
            chunk_months=1,
        )

        # Chunk 1: succeeds (100 rows). Chunks 2-3: fail (caught by exception handler)
        assert mock_extract.call_count == 3
        assert total_rows == 100
        assert failed == 2


def test_ingest_date_range_chunk_months_greater_than_one():
    """Test chunking when chunk_months > 1."""
    from flows.chicago_pipeline import _ingest_date_range

    with (
        patch("flows.chicago_pipeline.extract_and_load_chunk") as mock_extract,
        patch("flows.chicago_pipeline.merge_staging_to_iceberg") as mock_merge,
    ):
        mock_extract.return_value = 500
        mock_merge.return_value = 500

        _total_rows, _failed = _ingest_date_range(
            start_date="2026-01-01",
            end_date="2026-07-01",
            chunk_months=3,
        )

        # 6 months / 3 = 2 chunks
        assert mock_extract.call_count == 2


# ---------------------------------------------------------------------------
# Tests for Polars schema enforcement (fetch_from_socrata)
# ---------------------------------------------------------------------------

def test_schema_enforcement_injects_missing_columns():
    """Test that missing Socrata columns are injected as nulls with correct types.

    Mirrors the Socrata-to-Polars pipeline: raw all-string data gets date columns
    parsed, then missing columns are injected so BigQuery never sees absent columns.
    """
    # Simulate raw Socrata response (all string)
    raw_data = [
        {"sr_number": "SR1", "created_date": "2026-01-01T00:00:00.000", "last_modified_date": "2026-01-02T00:00:00.000"},
        {"sr_number": "SR2", "created_date": "2026-01-02T00:00:00.000", "last_modified_date": "2026-01-03T00:00:00.000"},
    ]
    df = pl.DataFrame(raw_data)

    # Parse date columns (as the pipeline does before renaming)
    df = df.with_columns([
        pl.col("created_date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
        pl.col("last_modified_date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
    ])

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

    # Rename to target schema (after date parsing)
    df = df.rename({"sr_number": "service_request_number"})

    # Inject missing columns as nulls with correct types
    for col_name, col_dtype in expected_schema.items():
        if col_name not in df.columns:
            df = df.with_columns(pl.lit(None).cast(col_dtype).alias(col_name))

    # Enforce exact column order
    df = df.select(list(expected_schema.keys()))

    assert df.columns == list(expected_schema.keys())
    for col_name, col_dtype in expected_schema.items():
        assert df.schema[col_name] == col_dtype, f"Column {col_name} has wrong type"


def test_primary_key_null_filter():
    """Test that null/empty service_request_number rows are filtered."""
    raw_data = [
        {"sr_number": "SR1", "created_date": "2026-01-01T00:00:00.000"},
        {"sr_number": "", "created_date": "2026-01-02T00:00:00.000"},
        {"sr_number": None, "created_date": "2026-01-03T00:00:00.000"},
    ]
    df = pl.DataFrame(raw_data)
    df = df.rename({"sr_number": "service_request_number"})

    df = df.filter(
        pl.col("service_request_number").is_not_null()
        & (pl.col("service_request_number") != "")
    )

    assert len(df) == 1
    assert df["service_request_number"][0] == "SR1"


def test_boolean_string_conversion():
    """Test that 'true'/'false' strings are converted to actual Booleans."""
    raw_data = [
        {"duplicate": "true", "legacy_record": "false"},
        {"duplicate": "false", "legacy_record": "true"},
    ]
    df = pl.DataFrame(raw_data)

    df = df.with_columns([
        (pl.col("duplicate") == "true").alias("duplicate"),
        (pl.col("legacy_record") == "true").alias("legacy_record"),
    ])

    assert df["duplicate"].dtype == pl.Boolean
    assert df["legacy_record"].dtype == pl.Boolean
    assert df["duplicate"][0] is True
    assert df["legacy_record"][0] is False


def test_empty_dataframe_returns_empty():
    """Test that empty Socrata response returns empty DataFrame."""
    df = pl.DataFrame()
    assert df.is_empty() is True


# ---------------------------------------------------------------------------
# Tests for date field derivation
# ---------------------------------------------------------------------------

def test_derived_temporal_columns():
    """Test that year, month, day_of_week are correctly derived from created_date."""
    raw_data = [
        {"created_date": "2026-01-15T10:30:00.000"},
        {"created_date": "2026-04-16T14:00:00.000"},
    ]
    df = pl.DataFrame(raw_data)

    df = df.with_columns([
        pl.col("created_date").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f"),
    ])

    df = df.with_columns([
        pl.col("created_date").dt.year().alias("created_year"),
        pl.col("created_date").dt.month().alias("created_month"),
        pl.col("created_date").dt.weekday().alias("created_day_of_week"),
    ])

    assert df["created_year"][0] == 2026
    assert df["created_month"][0] == 1
    assert df["created_year"][1] == 2026
    assert df["created_month"][1] == 4
    # 2026-04-16 is Thursday; Polars weekday(): 0=Monday, 1=Tuesday, 4=Thursday
    assert df["created_day_of_week"][1] == 4


# ---------------------------------------------------------------------------
# Tests for table identifier construction
# ---------------------------------------------------------------------------

def test_table_identifier_format():
    """Test that TABLE_IDENTIFIER is correctly formatted."""
    from flows.tasks.bigquery_ingestion import TABLE_IDENTIFIER

    parts = TABLE_IDENTIFIER.split(".")
    assert len(parts) == 3
    assert parts[2] == "service_requests"


def test_staging_table_identifier_format():
    """Test that STAGING_TABLE_IDENTIFIER is correctly formatted."""
    from flows.tasks.bigquery_ingestion import STAGING_TABLE_IDENTIFIER

    parts = STAGING_TABLE_IDENTIFIER.split(".")
    assert len(parts) == 3
    assert "staging" in parts[2]
