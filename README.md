# Capstone project for DE Zoomcamp 2026

Paste short intro and description

---

## Business Scenario and Data Product

***Simulated stakeholder interview***

"We get thousands of service requests every day across the city. Our biggest problems are: we don't know which departments are falling behind until it's already a political problem, ward aldermen call us asking why their neighborhoods wait longer for potholes to be fixed than other wards, and we have no early warning system for when we're about to breach our response commitments. We also get audited annually and have to manually pull spreadsheets to prove compliance. It takes days."

From that, the real needs are:
1. **Operational visibility** — what's the current state of open requests? Where are the backlogs building right now?
2. **SLA compliance tracking** — are departments meeting their response commitments? Which request types breach most often?
3. **Geographic equity analysis** — are some neighborhoods getting systematically slower service than others?
4. **Audit reporting** — automated, reproducible compliance reports instead of manual spreadsheet pulls

---

Those four needs map directly to pipeline components and architecture. Based on the above needs we need to formulate our own project requirements:

**Data product definition** — a clear one-paragraph statement of what this pipeline produces and who it serves.
+ The Chicago 311 Analytics Platform delivers daily-refreshed operational and compliance intelligence on city service requests to both operational staff and executive leadership. It enables monitoring of department SLA compliance, identification of geographic service inequities across Chicago's 77 community areas, tracking of open request backlogs, and generation of audit-ready compliance reports — replacing manual spreadsheet processes with automated, reproducible data products.

Stakeholder personas and their questions
This matters because operations and executive users ask fundamentally different questions, which shapes your mart design.
Operations team (daily)

Which departments have the most overdue open requests right now?
Which request types are breaching SLA most this week?
Is the backlog in my department growing or shrinking?
Which neighborhoods have the oldest unresolved requests?

Executive/management (weekly)

What is our overall SLA compliance rate this month vs last month?
Which community areas receive systematically slower service?
Are we improving or regressing on equity metrics over time?
What does our compliance look like for the annual audit period?

These two personas map to your two dashboards — Operational Dashboard for the operations team, SLA and Equity Dashboard for executives. The mart models feed them separately.

---

**SLA definitions** — not just the city's response time targets, but our pipeline's SLAs: when does data need to be ready, what's the acceptable staleness, what happens if ingestion fails

*Pipeline SLAs*

This is your pipeline's own service commitments — distinct from the city's 311 response targets.

CommitmentTargetMeasurementDaily ingestion completesBy 06:00 local timePrefect flow run statusData freshness on dashboardsMax 24h behind sourcemax(last_modified_date) in martdbt model run completesWithin 30 min of ingestiondbt run durationIngestion failure recoveryNext day's run catches up via incrementallast_modified_date watermarkBackfill completion (initial)2024–2025 loaded before daily schedule starts

---


**Data contract** — the schema guarantees we make to downstream consumers (dashboards, analysts): which columns are guaranteed non-null, what the grain of each table is, what uniqueness constraints hold

---


**Business rules** — the decisions that need to be documented: how duplicates are handled, how SLA targets are assigned, how geographic equity is measured

**Business rules — formally documented**

These are the decisions that must be written down so dbt tests can enforce them and future developers understand the intent.

BR-001: Duplicate handling
Duplicate requests (duplicate = true) are excluded from fct_sla_performance and both marts. They are retained in fct_service_requests with is_duplicate = true for lineage purposes. Rationale: measuring SLA on a duplicate request is meaningless since only the parent request drives resolution.
BR-002: Legacy record exclusion
Records with legacy_record = true (pre-2018 system) are filtered in stg_service_requests and never appear in any downstream model. Rationale: different data quality standards and incomplete fields make them incomparable to modern records.
BR-003: SLA target assignment
SLA targets are assigned by joining sr_type to the sla_targets seed. Request types with no matching seed entry receive a default target of 14 calendar days and are flagged with sla_target_source = 'default'. This flag surfaces in the mart so analysts know which types lack explicit targets.
BR-004: Resolution time calculation
resolution_days is calculated as calendar days between created_date and closed_date. Business days variant uses dim_date to exclude weekends and Illinois public holidays. Both measures are carried in fct_sla_performance. SLA breach is evaluated against calendar days to match Chicago's published targets.
BR-005: Geographic equity index
The equity index for a community area is defined as: area_avg_resolution_days / citywide_avg_resolution_days for the same sr_type and time period. A value of 1.0 means the area receives average service. Above 1.0 means slower than average (disadvantaged). Below 1.0 means faster. This makes geographic inequity quantifiable and comparable across request types.
BR-006: Open request age
For still-open requests, days_open is calculated as current_date - created_date. Requests open longer than their SLA target are flagged sla_at_risk = true in fct_service_requests regardless of whether they have technically breached yet.
BR-007: Incremental load watermark
Daily incremental runs fetch all records where last_modified_date > last_successful_run_date. The watermark is stored in Prefect as a flow variable and updated only on successful completion. Failed runs do not advance the watermark — the next run re-fetches from the last successful point.

---

## Data Modeling approach

**Starting point: what kind of data is this?**

Chicago 311 is an **event-driven** operational dataset. Each row is a discrete real-world event — a citizen reported something, the city responded. This maps naturally to a classic **Kimball star schema**: **facts** are events,**dimensions** describe the context of those events.

**Dimensions**

+ **`dim_date`**
The most standard dimension in any data warehouse. You need it because SQL date functions are expensive and inconsistent across engines, and because your SLA calculations need is_weekend and is_holiday flags that don't exist in raw timestamps. Generated as a date spine in dbt from 2024-01-01 to present, one row per day.

**Type: **SCD Type 0** — dates never change. January 15 2024 will always be a Tuesday. Fully static.

+ **`dim_request_type`**
Distinct sr_type values from the source, enriched with your seeded SLA targets and a category grouping (Infrastructure, Sanitation, Parks, etc.). This is where the business logic of "what kind of request is this and how quickly should it be resolved" lives.

**Type: SCD Type 1** — if you ever update an SLA target (say you decide potholes should be 5 days instead of 7), you just overwrite. You don't need history of what the SLA used to be for this project. In a real organization this might be Type 2, but that's overkill here.

+ **`dim_department`**
Distinct owner_department values enriched from your seed with display names and bureau groupings. Departments occasionally get renamed or reorganized in real city governments.

**Type: SCD Type 2** — this is the one dimension worth tracking history on. If "CDOT - Department of Transportation" gets renamed mid-year, you want historical requests to still show the old name so your trend analysis isn't broken. In practice for a 2-year dataset this probably won't happen, but modeling it as Type 2 demonstrates the pattern correctly.

+ **`dim_geography`**
This one is more interesting than it looks. The source has multiple geographic grains on every row: 
+ community_area (77 neighborhoods)
+ ward (50 political districts)
+ police_district
+ zip_code
+ raw latitude/longitude

You need to decide the grain of this dimension.
The right answer is **community area** as the primary grain — it's the most analytically meaningful, stable, and maps directly to neighborhood names from your seed. Ward and police district become attributes on the same row.

**Type: SCD Type 0** — Chicago's 77 community areas have fixed boundaries defined since 1920. They never change. Ward boundaries do change after redistricting (every 10 years), but since you're only covering 2024-2025 you won't cross a redistricting boundary.

+ **`dim_status`**
Worth having as a small dimension rather than a raw string in the fact table. Status values are a fixed controlled vocabulary: Open, Completed, Completed - Dup, Open - Dup. You can add display labels and a boolean is_resolved flag here.

**Type: SCD Type 0** — the status vocabulary is fixed.

**SCD strategy summary**

| Dimension | Type | Reason |
| :--- | :--- | :--- |
| **dim_date** | Type 0 | Immutable by definition |
| **dim_request_type** | Type 1 | SLA config changes overwrite, no history needed |
| **dim_department** | Type 2 | Dept names/structure can change, history matters |
| **dim_geography** | Type 0 | Community area boundaries are fixed |
| **dim_status** | Type 0 | Fixed controlled vocabulary |

---

**Fact Tables**

+ **`fct_service_requests`**

The core fact table. **Grain: one row per service request**. This is an **accumulating snapshot fact table** — not a transaction fact or a periodic snapshot. Here's why that distinction matters:
A transaction fact table captures one immutable event. But a service request isn't a single event — it has a lifecycle: created → assigned → in progress → closed. The row gets updated as the request moves through its lifecycle. That's the definition of an accumulating snapshot.



---


Let's go ahead and discuss the tech stack


## Stack and Requirements

+ **UV** as a package manager
+ **Docker Compose** for containerized setup
+ **Prefect Cloud (free tier)** for workflow orchestration
+ **Apache Iceberg via Pyiceberg** for Data Lakehouse capabilities
+ **Terraform** for GCP infrastructure management
+ **GCP resources: GCS, BigQuery with BigLake, Looker** 
  + **GCS** for storage
  + **BigQuery** as compute engine for Iceberg tables
  + **BigLake** for Iceberg catalog
  + **Looker** for dashboarding
+ **dbt** for data modeling and transformation, and data quality checks
+ **Great Expectations** for data quality checks
+ **Make** for easier management of the project

## Terraform setup

Infrastructure files:
```
infra/
├── main.tf          # The core resources (Bucket, Dataset, Connection)
├── variables.tf     # Project ID and Region
└── terraform.tfvars # Your actual values (not committed to Git)
```

```hcl

```

## Docker setup

Here's the `docker-compose.yml` setup:

```docker-compose

```

You need to run `docker compose up -d` or `make container-up` command to trigger Docker to configure all containers. Do this after you have your GCP infrastructure ready, i.e. after running `make infra-up`.

## AI assistant setup (If you're using Claude Code or other AI assistants)

**SKILL.md** file

### MCP Servers

+ Prefect MCP
+ Polars MCP
+ dbt MCP

## Data Ingestion

### Architecture Overview

```
┌─────────────┐     ┌─────────────────┐     ┌──────────────────────┐
│  Socrata    │────▶│  Prefect Flows  │────▶│  Apache Iceberg      │
│  API        │     │  (Orchestration)│     │  + SQL Catalog (Neon)│
└─────────────┘     └─────────────────┘     └──────────┬───────────┘
                                                        │
                                                        ▼
                                                ┌───────────────┐
                                                │  GCS Bucket   │
                                                │  (Parquet)    │
                                                └───────────────┘
```

### Common Concepts

**Iceberg SQL Catalog (Neon):** Stores table metadata pointing to GCS data files. Iceberg reads this metadata to locate actual Parquet files. When ingestion fails halfway, stale pointers may remain — cleanup Neon metadata before re-ingesting.

**Partitioning:** Year/month partitioning using `IdentityTransform` on derived `created_year` and `created_month` columns. Creates paths like `gs://bucket/year=2024/month=01/`.

**WAP Pattern (Daily Flow):** Write-Audit-Publish isolates new data on an audit branch before atomic promotion to main. Failed audits discard the branch — main stays clean.

### Flows Directory Structure

```
flows/
├── chicago_pipeline.py    # Three main flows: yearly_flow, daily_flow, backfill_flow
├── tasks/
│   ├── __init__.py        # Exports: extract_and_load_chunk, enable_wap,
│   │                       # create_audit_branch, audit_branch, publish_branch,
│   │                       # cleanup_branch, WAP_BRANCH
│   ├── ingestion.py        # Core ingestion task + WAP helpers
│   └── watermark.py        # (Deprecated - not used)
├── .pyiceberg.yaml         # Iceberg config (points to Neon)
└── DEPLOYMENT_GUIDE.md     # (Deleted - outdated)
```

### Prefect Setup

**1. Start Infrastructure:**
```bash
docker-compose up -d  # Starts Prefect server, worker, Redis, Postgres
```

**2. Create Work Pool:**
```bash
prefect work-pool create chicago-311-pool --type process
```

**3. Register Deployments:**
```bash
# Daily flow (scheduled at midnight UTC)
docker-compose exec flow-runner prefect deploy \
  flows/chicago_pipeline.py:daily_flow \
  --name daily-311 \
  --pool chicago-311-pool \
  --cron "0 0 * * *"
```

**Cron Expression Format:** `minute hour day-of-month month day-of-week` (all in UTC)

| Schedule | Cron Expression |
|----------|-----------------|
| Midnight UTC | `0 0 * * *` |
| 1 AM UTC | `0 1 * * *` |
| Every 6 hours | `0 */6 * * *` |

**To update an existing deployment's schedule:**
```bash
# Redeploy with new cron
docker-compose exec flow-runner prefect deploy \
  flows/chicago_pipeline.py:daily_flow \
  --name daily-311 \
  --pool chicago-311-pool \
  --cron "0 1 * * *"  # 1 AM UTC
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

### Yearly Flow

**Purpose:** Initial full-year ingestion (2024, 2025, partial 2026). Manual/once-per-year cadence.

**Process:** Iterates through 12 monthly chunks, each fetching up to 50K records from Socrata.

**Example:**
```bash
docker-compose exec flow-runner python -c "from flows.chicago_pipeline import yearly_flow; yearly_flow(2026)"
```

**Performance:** ~25-30 minutes per year. 1.9M+ rows.

---

### Daily Flow (WAP)

**Purpose:** Incremental updates using Write-Audit-Publish. Scheduled at midnight UTC.

**WAP Cycle:**

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Enable:   Set write.wap.enabled=true on table               │
│ 2. Create:   Make audit branch from main snapshot               │
│ 3. Write:    Append last 24h data to branch (3-4K rows)        │
│ 4. Audit:    Scan branch for date-filtered data quality checks  │
│ 5. Publish:  Atomically fast-forward main to branch snapshot   │
│ 6. Cleanup:  Delete audit branch                                │
└─────────────────────────────────────────────────────────────────┘
           ✅ Pass: Main updated   ❌ Fail: Branch discarded
```

**Audit Checks:**
- Row count ≥ minimum (default 1)
- No null `created_date` values
- Date-filtered scan (avoids reading entire table)

**Example:**
```python
from flows.chicago_pipeline import daily_flow
daily_flow()  # Automatically uses 24h lookback
```

**Performance:** ~2 minutes for 3-4K rows. Audit scans only new data via date filter.

---

### Backfill Flow

**Purpose:** Re-ingest arbitrary ranges for gap filling, corrections, or historical loads.

**Parameters:**
- `start_date`: YYYY-MM-DD (inclusive)
- `end_date`: YYYY-MM-DD (exclusive)
- `chunk_months`: Months per chunk, default 1

**Note:** Writes directly to main (no WAP).

**Example:**
```python
from flows.chicago_pipeline import backfill_flow
# One month
backfill_flow("2026-03-01", "2026-04-01")
# Quarterly chunks
backfill_flow("2024-01-01", "2025-01-01", chunk_months=3)
```

---

### Socrata API Considerations

**Why monthly chunks?** Socrata API times out on large requests. 50K records per chunk balances reliability vs. Prefect run count.

**Retry Logic:** `extract_and_load_chunk` has 5 retries with 30s delay between failed append attempts. Table is reloaded on retry to get fresh metadata.

**Derived Columns:** Socrata doesn't provide `created_year` — derived from `created_date.dt.year()` via Polars for partitioning.

---

### Data Summary

| Year | Rows | Notes |
|------|------|-------|
| 2024 | 1,913,929 | Full year |
| 2025 | 1,960,595 | Full year |
| 2026 | 498,531 | Jan–Apr only |
| **Total** | **4,373,055** | 291 MiB compressed |

**Storage Efficiency:** 4.37M records in Parquet = 291 MiB. Equivalent CSV would be 8-10x larger (~2.3–2.9 GB).


## Testing and development dependencies

mypy
ruff
pytest
sqlfluff