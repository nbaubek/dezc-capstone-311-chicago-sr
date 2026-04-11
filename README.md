# Capstone project for DE Zoomcamp 2026

Paste short intro and description

---

## Business Scenario and Data Product

***Simulated stakeholder interview***

> "*We get thousands of service requests every day across the city. Our biggest problems are: we don't know which departments are falling behind until it's already a political problem, ward aldermen call us asking why their neighborhoods wait longer for potholes to be fixed than other wards, and we have no early warning system for when we're about to breach our response commitments. We also get audited annually and have to manually pull spreadsheets to prove compliance. It takes days.*"

From that, the real needs are:
1. **Operational visibility** — what's the current state of open requests? Where are the backlogs building right now?
2. **SLA compliance tracking** — are departments meeting their response commitments? Which request types breach most often?
3. **Geographic equity analysis** — are some neighborhoods getting systematically slower service than others?
4. **Audit reporting** — automated, reproducible compliance reports instead of manual spreadsheet pulls

---

Those four needs map directly to pipeline components and architecture. Based on the above needs we need to formulate our own project requirements:

**Data product definition** — a clear one-paragraph statement of what this pipeline produces and who it serves.
+ The Chicago 311 Analytics Platform delivers daily-refreshed operational and compliance intelligence on city service requests to both operational staff and executive leadership. It enables monitoring of department SLA compliance, identification of geographic service inequities across Chicago's 77 community areas, tracking of open request backlogs, and generation of audit-ready compliance reports — replacing manual spreadsheet processes with automated, reproducible data products.

<br>

---

<br>

**Stakeholder personas and their questions**

This matters because operations and executive users ask fundamentally different questions, which shapes the mart design.


**Operations team (daily)**

+ Which departments have the most overdue open requests right now?
+ Which request types are breaching SLA most this week?
+ Is the backlog in my department growing or shrinking?
+ Which neighborhoods have the oldest unresolved requests?

**Executive/management (weekly)**

+ What is our overall SLA compliance rate this month vs last month?
+ Which community areas receive systematically slower service?
+ Are we improving or regressing on equity metrics over time?
+ What does our compliance look like for the annual audit period?

These two personas map to our two dashboards — **Operational Dashboard** for the operations team, **SLA and Equity Dashboard** for executives. The mart models feed them separately.

---

### Pipeline SLAs 

**SLA definitions** — not just the city's response time targets, but our pipeline's SLAs: 
+ **When does data need to be ready?**
+ **What's the acceptable staleness?**
+ **What happens if ingestion fails?**

These are our pipeline's own service commitments — distinct from the city's 311 response targets.

| Commitment | Target | Measurement |
| :--- | :--- | :--- |
| **Daily ingestion completes** | By 06:00 local time | Prefect flow run status |
| **Data freshness on dashboards** | Max 24h behind source | `max(last_modified_date)` in mart |
| **dbt model run completes** | Within 30 min of ingestion | dbt run duration |
| **Ingestion failure recovery** | Next day's run catches up via incremental | `last_modified_date` watermark |
| **Backfill completion (initial)** | 2024–2025 and part of 2026 loaded before daily schedule starts | Manual verification |

---


**Data contract** — the schema guarantees we make to downstream consumers (dashboards, analysts): which columns are guaranteed non-null, what the grain of each table is, what uniqueness constraints hold

**Schema Constraints and Guarantees**

`mart_operational`

| Column | Type | Guarantees |
| :--- | :--- | :--- |
| **snapshot_date** | date | Non-null, unique per `community_area` + `sr_type` + `snapshot_date` |
| **community_area** | integer | Non-null, always in range 1–77 |
| **community_area_name** | string | Non-null, from seed |
| **sr_type** | string | Non-null |
| **owner_department** | string | Non-null |
| **open_requests** | integer | Non-null, ≥ 0 |
| **avg_days_open** | float | Nullable — null when `open_requests` = 0 |
| **requests_older_than_sla** | integer | Non-null, ≥ 0 |
| **total_created_today** | integer | Non-null, ≥ 0 |

`mart_sla_performance`

| Column | Type | Guarantee |
| :--- | :--- | :--- |
| **period_month** | date | Non-null, first day of month |
| **sr_type** | string | Non-null |
| **owner_department** | string | Non-null |
| **community_area** | integer | Non-null |
| **community_area_name** | string | Non-null |
| **total_closed** | integer | Non-null, ≥ 0 |
| **total_breached** | integer | Non-null, ≥ 0 |
| **breach_rate** | float | Non-null, between 0.0 and 1.0 |
| **avg_resolution_days** | float | Nullable — null when `total_closed` = 0 |
| **avg_business_days_taken** | float | Nullable |
| **median_resolution_days** | float | Nullable |
| **equity_index** | float | Nullable — explained below |

---


**Business rules** — the decisions that need to be documented: how duplicates are handled, how SLA targets are assigned, how geographic equity is measured

**Business rules — formally documented**

These are the decisions that must be written down so dbt tests can enforce them and future developers understand the intent.

+ **BR-001: Duplicate handling**
  + Duplicate requests (duplicate = true) are excluded from `fct_sla_performance` and both marts. They are retained in `fct_service_requests` with `is_duplicate = true` for lineage purposes. Rationale: measuring SLA on a duplicate request is meaningless since only the parent request drives resolution.
+ **BR-002: Legacy record exclusion**
  + Records with legacy_record = true (pre-2018 system) are filtered in `stg_service_requests` and never appear in any downstream model. Rationale: different data quality standards and incomplete fields make them incomparable to modern records.
+ **BR-003: SLA target assignment**
  + SLA targets are assigned by joining `sr_type` to the `sla_targets` seed. Request types with no matching seed entry receive a default target of 14 calendar days and are flagged with `sla_target_source = 'default'`. This flag surfaces in the mart so analysts know which types lack explicit targets.
+ **BR-004: Resolution time calculation**
  + `resolution_days` is calculated as calendar days between `created_date` and `closed_date`. Business days variant uses `dim_date` to exclude weekends and Illinois public holidays. Both measures are carried in `fct_sla_performance`. SLA breach is evaluated against calendar days to match Chicago's published targets.
+ **BR-005: Geographic equity index**
  + The equity index for a community area is defined as: `area_avg_resolution_days / citywide_avg_resolution_days` for the same `sr_type` and time period. A value of 1.0 means the area receives average service. Above 1.0 means slower than average (disadvantaged). Below 1.0 means faster. This makes geographic inequity quantifiable and comparable across request types.
+ **BR-006: Open request age**
  + For still-open requests, days_open is calculated as `current_date` - `created_date`. Requests open longer than their SLA target are flagged `sla_at_risk = true` in `fct_service_requests` regardless of whether they have technically breached yet.
+ **BR-007: Incremental load watermark**
  + Daily incremental runs fetch all records where `last_modified_date` > `last_successful_run_date`. The watermark is stored in Prefect as a flow variable and updated only on successful completion. Failed runs do not advance the watermark — the next run re-fetches from the last successful point.

---

## Data Modeling approach

**Starting point: what kind of data is this?**

Chicago 311 is an **event-driven** operational dataset. Each row is a discrete real-world event — a citizen reported something, the city responded. This maps naturally to a classic **Kimball star schema**: **facts** are events,**dimensions** describe the context of those events.

**Dimensions**

1. **`dim_date`**
The most standard dimension in any data warehouse. You need it because SQL date functions are expensive and inconsistent across engines, and because our SLA calculations need `is_weekend` and `is_holiday` flags that don't exist in raw timestamps. Generated as a date spine in dbt from 2024-01-01 to present, one row per day.
    + **Type: SCD Type 0** — dates never change. January 15 2024 will always be a Tuesday. Fully static.

2. **`dim_request_type`**
Distinct `sr_type` values from the source, enriched with our seeded SLA targets and a category grouping (Infrastructure, Sanitation, Parks, etc.). This is where the business logic of "what kind of request is this and how quickly should it be resolved" lives.
    + **Type: SCD Type 1** — if you ever update an SLA target (say you decide potholes should be 5 days instead of 7), you just overwrite. You don't need history of what the SLA used to be for this project. In a real organization this might be Type 2, but that's overkill here.

3. **`dim_department`**
Distinct owner_department values enriched from our seed with display names and bureau groupings. Departments occasionally get renamed or reorganized in real city governments.
    + **Type: SCD Type 2** — this is the one dimension worth tracking history on. If "CDOT - Department of Transportation" gets renamed mid-year, you want historical requests to still show the old name so our trend analysis isn't broken. In practice for a 2-year dataset this probably won't happen, but modeling it as Type 2 demonstrates the pattern correctly.

4. **`dim_geography`**
This one is more interesting than it looks. The source has multiple geographic grains on every row: 
    + `community_area` (77 neighborhoods)
    + `ward` (50 political districts)
    + `police_district`
    + `zip_code`
    + raw `latitude/longitude`

    + You need to decide the grain of this dimension.
The right answer is **community area** as the primary grain — it's the most analytically meaningful, stable, and maps directly to neighborhood names from our seed. Ward and police district become attributes on the same row.

    + **Type: SCD Type 0** — Chicago's 77 community areas have fixed boundaries defined since 1920. They never change. Ward boundaries do change after redistricting (every 10 years), but since we're only covering 2024-2025-2026 we won't cross a redistricting boundary.

5. **`dim_status`**
Worth having as a small dimension rather than a raw string in the fact table. Status values are a fixed controlled vocabulary: Open, Completed, Completed - Dup, Open - Dup. You can add display labels and a boolean is_resolved flag here.

    + **Type: SCD Type 0** — the status vocabulary is fixed.

---

**SCD strategy summary**

| Dimension | Type | Reason |
| :--- | :--- | :--- |
| **dim_date** | Type 0 | Immutable by definition |
| **dim_request_type** | Type 1 | SLA config changes overwrite, no history needed |
| **dim_department** | Type 2 | Dept names/structure can change, history matters |
| **dim_geography** | Type 0 | Community area boundaries are fixed |
| **dim_status** | Type 0 | Fixed controlled vocabulary |

<br>

---

<br>

**Fact Tables**

+ **`fct_service_requests`**

The core fact table. **Grain: one row per service request**. This is an **accumulating snapshot fact table** — not a transaction fact or a periodic snapshot. Here's why that distinction matters:
A transaction fact table captures one immutable event. But a service request isn't a single event — it has a lifecycle: 

```
created → assigned → in progress → closed 
```

The row gets updated as the request moves through its lifecycle. That's the definition of an accumulating snapshot.

---


Let's go ahead and discuss the tech stack.


## Stack and Requirements

+ **UV** as a package manager
+ **Docker Compose** for containerized setup
+ **Prefect Cloud (free tier)** for workflow orchestration
+ **Apache Iceberg via BigLake/BigQuery/GCS** for Data Lakehouse capabilities
+ **GCP resources: GCS, BigQuery with BigLake, Looker** 
  + **GCS** for storage
  + **BigQuery** as compute engine for Iceberg tables
  + **BigLake** for Iceberg catalog
  + **Looker** for dashboarding
+ **Terraform** for GCP infrastructure management
+ **dbt** for data modeling and transformation, and data quality checks
+ **Great Expectations** for data quality checks
+ **Make** for easier management of the project
+ **Github CI/CD** for automating the building, testing, and deployment of code changes and thereby faster code releases
+ **mypy** for type checking, **pytest** for Python testing, **sqlfluff** for testing SQL in dbt models, and **ruff** as Python linter and formatter

<br>

A high-level view of how the architecture works:

| Components | Implementation |
| :--- | :--- |
| **Catalog** | BigLake Metastore (built into BigQuery) |
| **Ingestion** | BigQuery Python SDK (`google-cloud-bigquery`) |
| **WAP** | Audit table swap |
| **Metadata** | Stored by BigQuery in BigLake |



---


## Terraform setup

Infrastructure files:
```
infra/
├── main.tf              # Core resources (GCS Bucket, BigQuery Dataset, Connection)
├── variables.tf         # Project ID, Region, Bucket Name, Dataset ID
├── terraform.tfvars     # Actual values (NOT committed to Git)
├── outputs.tf           # Output variables for .env configuration
├── providers.tf         # Terraform and GCP provider configuration
└── .gitignore           # Git ignore patterns for Terraform state
```

## Docker setup

```docker-compose
services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: prefect
      POSTGRES_PASSWORD: prefect
      POSTGRES_DB: prefect
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - chicago-311-sr-net

  redis:
    image: redis:7
    volumes:
      - redis_data:/data
    networks:
      - chicago-311-sr-net

  prefect-server:
    image: prefecthq/prefect:3-latest
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      PREFECT_API_DATABASE_CONNECTION_URL: postgresql+asyncpg://prefect:prefect@postgres:5432/prefect
      PREFECT_SERVER_API_HOST: 0.0.0.0
      PREFECT_SERVER_UI_API_URL: http://localhost:4200/api
      PREFECT_MESSAGING_BROKER: prefect_redis.messaging
      PREFECT_MESSAGING_CACHE: prefect_redis.messaging
      PREFECT_REDIS_MESSAGING_HOST: redis
      PREFECT_REDIS_MESSAGING_PORT: 6379
    ports:
      - "4200:4200"
    networks:
      - chicago-311-sr-net

  prefect-services:
    image: prefecthq/prefect:3-latest
    depends_on:
      prefect-server:
        condition: service_healthy
    environment:
      PREFECT_API_DATABASE_CONNECTION_URL: postgresql+asyncpg://prefect:prefect@postgres:5432/prefect
      PREFECT_MESSAGING_BROKER: prefect_redis.messaging
      PREFECT_MESSAGING_CACHE: prefect_redis.messaging
    command: prefect server services start
    networks:
      - chicago-311-sr-net

  prefect-worker:
    image: prefecthq/prefect:3-latest
    depends_on:
      prefect-server:
        condition: service_healthy
    environment:
      PREFECT_API_URL: http://prefect-server:4200/api
    command: prefect worker start --pool local-pool
    networks:
      - chicago-311-sr-net

  flow-runner:
    build:
      context: .
      dockerfile: files.Dockerfile
    depends_on:
      prefect-server:
        condition: service_healthy
    env_file:
      - .env
    environment:
      PREFECT_API_URL: http://prefect-server:4200/api
    command: prefect worker start --pool chicago-311-pool
    volumes:
      - ./flows:/app/flows:ro
      - ./.pyiceberg.yaml:/app/.pyiceberg.yaml:ro
      - ./application_default_credentials.json:/app/secrets/application_default_credentials.json:ro
    networks:
      - chicago-311-sr-net

volumes:
  postgres_data:
  redis_data:

networks:
  chicago-311-sr-net:
    driver: bridge
```

You need to run `docker compose up -d` or `make container-up` command to trigger Docker to configure all containers. Do this after you have your GCP infrastructure ready, i.e. after running `make infra-up`.

## AI assistant setup (If you're using Claude Code or other AI assistants)

### MCP Servers

+ Prefect MCP
+ Polars MCP
+ dbt MCP

## Data Ingestion

### Architecture Overview

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│  Socrata    │────▶│  Prefect Flows  │────▶│  BigQuery Iceberg   │
│  API        │     │  (Orchestration)│     │  + BigLake Metastore│
└─────────────┘     └─────────────────┘     └──────────┬───────────┘
                                                        │
                                                        ▼
                                                ┌───────────────┐
                                                │  GCS Bucket   │
                                                │  (Parquet)    │
                                                └───────────────┘
```

### Common Concepts

**BigLake Metastore:** BigQuery's native Iceberg catalog service. Manages Iceberg table metadata automatically without needing a third-party catalog. Tables are created and managed directly through BigQuery SQL.

**Partitioning:** Year/month partitioning using derived `created_year` and `created_month` columns. Creates paths like `gs://bucket/year=2024/month=01/`.

**WAP Pattern (Daily Flow):** BigQuery doesn't support Iceberg's native branching, so we use **audit table swap** pattern:
1. Write new data to audit table
2. Validate audit data
3. Insert audit data into main table (with duplicate prevention)
4. Clear audit table for next run

## Prefect Setup

### Flows Directory Structure

```
flows/
├── chicago_pipeline.py        # Three main flows: yearly_flow, daily_flow, backfill_flow
├── tasks/
│   ├── __init__.py            # Exports BigQuery ingestion functions
│   └── bigquery_ingestion.py  # Core BigQuery/BigLake ingestion tasks
```

### Running the flows

**1. Start Infrastructure:**
```bash
docker-compose up -d  # Starts Prefect server, worker, Redis, Postgres
```

**2. Create Work Pool:**
```bash
# Create the process-based work pool for flow-runner container
docker-compose exec flow-runner prefect work-pool create chicago-311-pool --type process
```

**3. Register Deployments:**
```bash
# Daily flow (scheduled at midnight UTC)
docker-compose exec flow-runner prefect deploy \
  flows/chicago_pipeline.py:daily_flow \
  --name daily-311 \
  --pool chicago-311-pool \
  --cron "0 0 * * *"

# Yearly flow (manual trigger)
docker-compose exec flow-runner prefect deploy \
  flows/chicago_pipeline.py:yearly_flow \
  --name yearly-311 \
  --pool chicago-311-pool

# Backfill flow (manual trigger)
docker-compose exec flow-runner prefect deploy \
  flows/chicago_pipeline.py:backfill_flow \
  --name backfill-311 \
  --pool chicago-311-pool
```

**Cron Expression Format:** `minute hour day-of-month month day-of-week` (all in UTC)

| Schedule | Cron Expression |
|----------|-----------------|
| Midnight UTC | `0 0 * * *` |
| 1 AM UTC | `0 1 * * *` |
| Every 6 hours | `0 */6 * * *` |

**To update an existing deployment's schedule:**
```bash
docker-compose exec flow-runner prefect deploy \
  flows/chicago_pipeline.py:daily_flow \
  --name daily-311 \
  --pool chicago-311-pool \
  --cron "0 6 * * *"  # 6 AM
```

**Verify schedule via UI:** Open http://localhost:4200 → Deployments → daily-311 → Schedule tab

**4. Run Flows:**

| Method | Command |
|--------|---------|
| Manual via CLI | `prefect deployment run 'Daily 311 Ingestion/daily-311'` |
| Manual via UI | Open http://localhost:4200 |
| Ad-hoc via Docker | `docker-compose exec flow-runner python -c "from flows.chicago_pipeline import yearly_flow; yearly_flow(2026)"` |

**Important:** Only run 1 flow at a time. Concurrent writes cause `CommitFailedException`.

---

#### Yearly Flow

**Purpose:** Initial full-year ingestion (2024, 2025, partial 2026). Manual/once-per-year cadence.

**Process:** Iterates through 12 monthly chunks, using pagination to fetch all records in each chunk (handles 160-200K+ rows per month).

**Example:**
```bash
docker-compose exec flow-runner python -c "from flows.chicago_pipeline import yearly_flow; yearly_flow(2026)"
```

**Performance:** ~25-30 minutes per year. 1.9M+ rows.

---

#### Daily Flow (WAP - Audit Table Swap)

**Purpose:** Incremental updates using Write-Audit-Publish. Scheduled at midnight UTC.

**Process:** Fetches all records modified in the last 24 hours using `last_modified_date` filter (catches closed requests, status updates, corrections — not just newly created requests).

**WAP Cycle (BigQuery Audit Table Swap):**

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Setup:   Ensure BigQuery dataset, tables, and audit table exist │
│ 2. Clear:   Clear audit table for new data                      │
│ 3. Write:    Append last 24h data to audit table (3-4K rows)   │
│ 4. Audit:    Validate audit table (row count, null checks)     │
│ 5. Merge:    Insert audit data into main table (deduplicated)   │
│ 6. Cleanup:  Clear audit table for next run                     │
└─────────────────────────────────────────────────────────────────┘
           ✅ Pass: Main updated   ❌ Fail: Audit table cleared
```

**Audit Checks:**
- Row count ≥ minimum (default 1)
- No null `created_date` values
- Date-filtered validation (avoids reading entire table)

**Example:**
```python
from flows.chicago_pipeline import daily_flow
daily_flow()  # Automatically uses 24h lookback on last_modified_date
```

**Performance:** ~2 minutes for 3-4K rows. Validation uses date filter for efficiency.

---

#### Backfill Flow

**Purpose:** Re-ingest arbitrary ranges for gap filling, corrections, or historical loads.

**Parameters:**
- `start_date`: YYYY-MM-DD (inclusive)
- `end_date`: YYYY-MM-DD (exclusive)
- `chunk_months`: Months per chunk, default 1
- `date_field`: Which date field to filter on (default "created_date"). Use "last_modified_date" for correction backfills.

**Note:** Writes directly to main table (no WAP). Can filter on `created_date` for new data or `last_modified_date` for corrections.

**Example:**
```python
from flows.chicago_pipeline import backfill_flow
# One month of new requests
backfill_flow("2026-03-01", "2026-04-01")
# Quarterly chunks
backfill_flow("2024-01-01", "2025-01-01", chunk_months=3)
# Correction backfill - all changes to existing records in a period
backfill_flow("2026-03-01", "2026-04-01", date_field="last_modified_date")
```

---

## Data Summary

This [link](https://dev.socrata.com/foundry/data.cityofchicago.org/v6vf-nfxy) has the description of columns.

### **Socrata API Considerations**

**Why monthly chunks?** Socrata API may timeout on very large requests. Monthly chunks balance reliability vs. Prefect run count.

**Pagination:** The `fetch_from_socrata` task uses pagination to automatically loop through all batches until all records in the date range are fetched. No manual chunk management needed for large months (160-200K+ rows).

**Retry Logic:** `write_to_bigquery` has 5 retries with 30s delay between failed attempts. `extract_and_load_chunk` handles retries at the individual task level (`fetch_from_socrata` has 3 retries, `write_to_bigquery` has 5 retries).

**Derived Columns:** `created_year` and `created_month` are derived from `created_date.dt.year()` and `created_date.dt.month()` via Polars for partitioning. `created_day_of_week` is also derived from `created_date.dt.weekday()` (0=Monday to 6=Sunday) — Socrata sends day name strings which cannot be directly cast to integers.

---

### **Data Schema**

*Raw Socrata API Response (all 36 columns are TEXT/STRING):*

| Column | Description |
|--------|-------------|
| `sr_number` | Unique service request identifier |
| `sr_type` | Type of service request (e.g., "Pothole Repair") |
| `sr_short_code` | Short code for the service type |
| `owner_department` | Department responsible for the request |
| `status` | Current status (e.g., "Open", "Completed") |
| `origin` | Source of the request (e.g., "311 Center", "Mobile App") |
| `created_date` | When the request was submitted (ISO timestamp) |
| `last_modified_date` | Last update timestamp |
| `closed_date` | When the request was resolved (NULL if open) |
| `street_address` | Street address |
| `city` | City (always "Chicago") |
| `state` | State (always "IL") |
| `zip_code` | ZIP code |
| `street_number` | Street number |
| `street_direction` | Street direction (N, S, E, W) |
| `street_name` | Street name |
| `street_type` | Street type (St, Ave, Blvd, etc.) |
| `duplicate` | Flag for duplicate requests ("true"/"false") |
| `legacy_record` | Flag for pre-2018 records ("true"/"false") |
| `community_area` | Chicago community area (1–77) |
| `ward` | Political ward (1–50) |
| `police_sector` | Police sector |
| `police_district` | Police district |
| `police_beat` | Police beat |
| `precinct` | Precinct |
| `created_hour` | Hour of day (0–23) |
| `created_day_of_week` | Day of week (0–6) |
| `x_coordinate` | X coordinate (State Plane) |
| `y_coordinate` | Y coordinate (State Plane) |
| `latitude` | Latitude coordinate |
| `longitude` | Longitude coordinate |
| `created_department` | Department that created the request |
| `electrical_district` | Electrical district |
| `electricity_grid` | Electricity grid identifier |
| `parent_sr_number` | Parent service request number |

*Note*: `city`, `state`, and `location` columns are also received but dropped during storage (always "Chicago" and "IL"). 

`location` is GeoJSON point location and is a combination of `latitude` and `longitude`, therefore it can also be derived if needed.

---

**Schema Changes After Polars Type Casting:**


| Column | Original Type | After Casting | Notes |
|--------|---------------|----------------|-------|
| `service_request_number` | TEXT | STRING | Renamed from `sr_number`; primary key |
| `sr_type` | TEXT | STRING | |
| `sr_short_code` | TEXT | STRING (nullable) | Empty string → NULL |
| `owner_department` | TEXT | STRING | |
| `current_status` | TEXT | STRING | Renamed from `status` |
| `origin` | TEXT | STRING (nullable) | Empty string → NULL |
| `created_date` | TEXT | TIMESTAMP | ISO timestamp → datetime |
| `last_modified_date` | TEXT | TIMESTAMP | ISO timestamp → datetime |
| `closed_date` | TEXT | TIMESTAMP (nullable) | Empty string → NULL |
| `street_address` | TEXT | STRING | Empty string → NULL |
| `zip_code` | TEXT | STRING (nullable) | Empty string → NULL |
| `street_number` | TEXT | STRING | Empty string → NULL |
| `street_direction` | TEXT | STRING | Empty string → NULL |
| `street_name` | TEXT | STRING | Empty string → NULL |
| `street_type` | TEXT | STRING | Empty string → NULL |
| `duplicate` | TEXT | BOOLEAN | "true"/"false" string → True/False |
| `legacy_record` | TEXT | BOOLEAN | "true"/"false" string → True/False |
| `community_area` | TEXT | INT64 | Empty string → NULL |
| `ward` | TEXT | INT64 (nullable) | Empty string → NULL |
| `police_sector` | TEXT | STRING | Empty string → NULL |
| `police_district` | TEXT | STRING (nullable) | Empty string → NULL |
| `police_beat` | TEXT | STRING (nullable) | Empty string → NULL |
| `precinct` | TEXT | STRING (nullable) | Empty string → NULL |
| `created_hour` | TEXT | INT64 (nullable) | Empty string → NULL |
| `created_day_of_week` | — | INT64 | Derived from `created_date.dt.weekday()` |
| `x_coordinate` | TEXT | FLOAT64 (nullable) | Empty string → NULL |
| `y_coordinate` | TEXT | FLOAT64 (nullable) | Empty string → NULL |
| `latitude` | TEXT | FLOAT64 (nullable) | Empty string → NULL |
| `longitude` | TEXT | FLOAT64 (nullable) | Empty string → NULL |
| `created_department` | TEXT | STRING (nullable) | Empty string → NULL |
| `electrical_district` | TEXT | STRING (nullable) | Empty string → NULL |
| `electricity_grid` | TEXT | STRING (nullable) | Empty string → NULL |
| `parent_sr_number` | TEXT | STRING (nullable) | Empty string → NULL |
| `created_year` | — | INT64 | Derived: `YEAR(created_date)` |
| `created_month` | — | INT64 | Derived: `MONTH(created_date)` |

**Columns Renamed (during ingestion):**

| Original Column | New Column | Reason |
|-----------------|------------|--------|
| `sr_number` | `service_request_number` | More descriptive name |
| `status` | `current_status` | More descriptive name |

**Columns Dropped (Not Stored in Iceberg tables):**

| Column | Reason |
|--------|--------|
| `city` | Always "Chicago" - redundant |
| `state` | Always "IL" - redundant |
| `location` | GeoJSON point - redundant with lat/long columns |

**Derived Columns Added:**

| Column | Calculation | Purpose |
|--------|------------|---------|
| `created_year` | `YEAR(created_date)` | Partitioning by year |
| `created_month` | `MONTH(created_date)` | Partitioning by month |
| `created_day_of_week` | `created_date.dt.weekday()` | Day of week (0=Monday, 6=Sunday) |


*Note:* BigQuery does not enforce nullability constraints. All columns in BigQuery are nullable regardless of schema definitions. Nullable means the column can contain NULL values (missing data).

---

**Row Counts by Year:**


| Year | Rows | Notes |
|------|------|-------|
| 2024 | 1,913,929 | Full year |
| 2025 | 1,960,595 | Full year |
| 2026 | 498,531-and counting | Jan–Apr only |
| **Total** | **4,373,055** | 291 MiB compressed |

**Storage Efficiency:** ~4.4M records in Parquet = 291 MiB. Equivalent CSV would be 8-10x larger (~2.3–2.9 GB).

---

## Testing and development dependencies

mypy
ruff
pytest
sqlfluff

---

## How do I measure that the pipeline is reliable (Whether with AI assistant or without)?

**The "Defensive Checklist"**

For every piece of code and the architecture as a whole, ask yourself:

| Category           | The "Unknown" Question                                                      |
|-------------------|----------------------------------------------------------------------------|
| Idempotency       | If I run this script twice, will it double the data or stay the same?      |
| Schema Evolution  | What happens if a new column is added to the source tomorrow?              |
| Partial Failure   | If the job crashes at 50%, do I have a mess of "half-written" data?        |
| Observation       | If this fails at 3:00 AM, will the logs tell me exactly why?               |



