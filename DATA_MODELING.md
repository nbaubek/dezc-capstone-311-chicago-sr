# Data Modeling Guide: Chicago 311 Analytics Platform

## Executive Summary

This document describes the data modeling process for the Chicago 311 Analytics Platform, from raw data ingestion to actionable business intelligence. The platform transforms raw 311 service request data into a Kimball-style data warehouse that powers two strategic dashboards: an **Operational Dashboard** for daily monitoring and an **SLA & Equity Dashboard** for executive decision-making.

---

## Business Problem

### The Challenge

Chicago receives thousands of service requests daily across 77 community areas and 50 political wards. City leadership faces critical questions:

> "We don't know which departments are falling behind until it's already a political problem. Ward aldermen call us asking why their neighborhoods wait longer for potholes to be fixed than other wards. We have no early warning system for when we're about to breach our response commitments. We also get audited annually and have to manually pull spreadsheets to prove compliance—this takes days."

### The Solution

The Chicago 311 Analytics Platform delivers:
1. **Operational visibility** — Real-time view of open requests, backlogs, and emerging issues
2. **SLA compliance tracking** — Automated monitoring of department performance against response time commitments
3. **Geographic equity analysis** — Quantitative measurement of service disparities across community areas
4. **Audit-ready reporting** — Reproducible compliance reports on demand, not manual spreadsheet pulls

---

## Data Journey: From Source to Insight

### 1. Source Data

**Chicago 311 Service Requests (Socrata API)**

- **Source**: City of Chicago Open Data Portal (`data.cityofchicago.org`)
- **Dataset**: `311 Service Requests - v6vf-nfxy`
- **Volume**: ~4.4M records (2024–2026), growing daily
- **Freshness**: Near real-time (API provides `last_modified_date`)
- **Schema**: 36+ columns capturing request lifecycle, geography, and metadata

**Key Source Columns:**
| Column | Type | Description |
|--------|------|-------------|
| `sr_number` | STRING | Unique service request identifier |
| `created_date` | TIMESTAMP | When the request was submitted |
| `closed_date` | TIMESTAMP | When the request was resolved (NULL if open) |
| `sr_type` | STRING | Type of service request (e.g., "Pothole Repair") |
| `status` | STRING | Current status (e.g., "Open", "Completed") |
| `owner_department` | STRING | Department responsible |
| `community_area` | STRING | Chicago community area (1–77) |
| `ward` | INT64 | Political ward (1–50) |
| `duplicate` | BOOLEAN | Flag for duplicate requests |
| `legacy_record` | BOOLEAN | Flag for pre-2018 records (different quality standards) |

**Derived Columns Created During Ingestion:**
- `created_year`, `created_month` — Partition keys for efficient querying
- `created_hour`, `created_day_of_week` — Temporal analysis dimensions
- `x_coordinate`, `y_coordinate` — Spatial data for GIS integration

### 2. Ingestion Layer

**Technology**: BigQuery (with BigLake Metastore for Iceberg support)

**Ingestion Flows:**

| Flow | Purpose | Schedule | Pattern |
|------|---------|----------|---------|
| `yearly_flow` | Initial load of full calendar year | Manual (once per year) | Monthly chunks |
| `daily_flow` | Incremental updates for last 24 hours | Daily at midnight UTC | WAP (Write-Audit-Publish) |
| `backfill_flow` | Re-ingest arbitrary date ranges | Manual (gap fixing) | Configurable chunks |

**Write-Audit-Publish (WAP) Pattern:**

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

**Data Quality Checks (Audit):**
- Row count ≥ minimum expected
- No null `created_date` values in new data
- Date-filtered validation (avoids scanning entire table)

### 3. Staging Layer

**Purpose**: Clean, cast, and rename source data for downstream use.

**Model**: `stg_service_requests`

**Transformations:**
```sql
-- Key transformations performed
1. Rename source columns to consistent naming
   - sr_number → service_request_number
   - status → current_status

2. Cast to appropriate data types
   - Dates: STRING → TIMESTAMP
   - Numbers: STRING → INTEGER/FLOAT
   - Booleans: STRING → BOOLEAN

3. Filter out data quality issues
   - Remove legacy records (pre-2018)
   - Remove duplicates from analysis (keep for lineage)

4. Add derived columns
   - created_year = YEAR(created_date)
   - created_month = MONTH(created_date)
```

**Output**: Cleaned, typed table with all source columns preserved for flexibility.

### 4. Intermediate Layer

**Purpose**: Apply business logic and prepare data for dimensional modeling.

**Model**: `int_sla_performance`

**Key Business Logic:**

| Logic | Description |
|-------|-------------|
| **SLA Target Assignment** | Join `sr_type` to `sla_targets` seed. If no match, use 14-day default and flag `sla_target_source = 'default'` |
| **Resolution Time Calculation** | `resolution_days = DATE_DIFF(closed_date, created_date)` (calendar days) |
| **Business Days Variant** | Exclude weekends and Illinois holidays using `dim_date` |
| **SLA Breach Detection** | `is_sla_breach = (resolution_days > sla_target_days)` |
| **SLA At-Risk Detection** | `sla_at_risk = (days_open > sla_target_days)` for open requests |
| **Equity Index** | `equity_index = area_avg_resolution_days / citywide_avg_resolution_days` by sr_type and period |

**Output**: Enriched fact table with SLA metrics ready for dimensional modeling.

### 5. Mart Layer

**Purpose**: Kimball star schema tailored to specific business questions.

#### 5.1 Dimensions

| Dimension | Type | Description | Key Attributes |
|-----------|------|-------------|----------------|
| `dim_date` | SCD Type 0 | Calendar dimension for temporal analysis | date, day_of_week, is_weekend, is_holiday, year, month, quarter |
| `dim_request_type` | SCD Type 2 | Service request types with SLA targets | sr_type, category, sla_target_days, sla_category, effective_start_date, effective_end_date |
| `dim_department` | SCD Type 2 | City departments handling requests | department, contact_email, parent_department, service_types |
| `dim_geography` | SCD Type 0 | Chicago geographic entities | community_area, ward, zip_code, population, demographic_data |
| `dim_status` | SCD Type 0 | Request status taxonomy | status, status_category, is_open, is_closed, is_in_progress |

#### 5.2 Facts

**`fct_service_requests`** (Accumulating Snapshot Fact Table)

This is the core fact table that captures the lifecycle of each service request.

**Grain**: One row per service request, updated as the request moves through its lifecycle.

**Key Measures:**
- `days_open`: Current time since creation (for open requests) or final duration (for closed requests)
- `resolution_days`: Final time to resolution (NULL until closed)
- `sla_target_days`: Target response time for this request type
- `is_sla_breach`: Did the request exceed its SLA target?
- `sla_at_risk`: Is an open request approaching SLA breach?
- `equity_index`: Relative performance vs citywide average for this type
- `duplicate_flag`: Is this a duplicate request?
- `legacy_flag`: Is this a legacy record (pre-2018)?

**Foreign Keys:**
- `date_key` → `dim_date`
- `request_type_key` → `dim_request_type`
- `department_key` → `dim_department`
- `geography_key` → `dim_geography`
- `status_key` → `dim_status`

#### 5.3 Dashboard Marts

**`mart_operational_dashboard`** (Operational Team)

Purpose: Daily operational monitoring and response.

**Key Metrics:**
- `open_requests_total`: Current open requests
- `open_requests_over_sla`: Open requests exceeding SLA target
- `open_requests_at_risk`: Open requests approaching SLA breach
- `average_response_time`: Average time to close (last 7 days)
- `backlog_by_department`: Open requests by department
- `new_requests_24h`: New requests in last 24 hours
- `resolved_requests_24h`: Requests resolved in last 24 hours

**Example Query:**
```sql
SELECT
    owner_department,
    COUNT(*) as open_requests,
    SUM(CASE WHEN sla_at_risk THEN 1 ELSE 0 END) as at_risk
FROM fct_service_requests
WHERE status_key = (SELECT status_key FROM dim_status WHERE is_open = TRUE)
GROUP BY owner_department
ORDER BY open_requests DESC
```

**`mart_sla_equity_dashboard`** (Executive/Management)

Purpose: Weekly strategic reporting on SLA compliance and geographic equity.

**Key Metrics:**
- `sla_compliance_rate`: % of requests meeting SLA target
- `sla_compliance_by_department`: Department-level performance
- `sla_compliance_by_request_type`: Which request types breach most often
- `equity_index_by_area`: Neighborhood performance vs city average
- `trend_sla_compliance`: SLA performance over time (monthly)
- `top_breaching_areas`: Community areas with worst performance
- `trending_breaches`: Request types with increasing breach rates

**Example Query:**
```sql
SELECT
    community_area,
    sr_type,
    AVG(equity_index) as avg_equity_index,
    COUNT(*) as request_count,
    AVG(resolution_days) as avg_resolution_days
FROM fct_service_requests
WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY community_area, sr_type
HAVING avg_equity_index > 1.2  -- 20% worse than average
ORDER BY avg_equity_index DESC
```

---

## Data Model Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Staging Layer                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│  stg_service_requests (cleaned source data)                                │
│  - All source columns                                                     │
│  - Type casting applied                                                   │
│  - Legacy and duplicate records flagged                                   │
└────────────────────────────┬──────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Intermediate Layer                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│  int_sla_performance                                                        │
│  - SLA targets assigned                                                    │
│  - Resolution times calculated                                             │
│  - SLA breach detection                                                    │
│  - Equity index computed                                                   │
└────────────────────────────┬──────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      Mart Layer (Star Schema)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  Dimensions:                    Fact:                                      │
│  ┌─────────────┐                ┌─────────────────────┐                   │
│  │ dim_date    │◄───────────────┤                     │                   │
│  │             │                │ fct_service_        │                   │
│  │ - date      │                │   requests          │                   │
│  │ - is_weekend│                │                     │                   │
│  └─────────────┘                │ - days_open         │                   │
│  ┌─────────────┐                │ - resolution_days   │                   │
│  │ dim_request │◄───────────────┤ - is_sla_breach     │                   │
│  │   _type     │                │ - equity_index      │                   │
│  │             │                │                     │                   │
│  │ - sla_target│◄───────────────┤                     │                   │
│  └─────────────┘                └──────────┬──────────┘                   │
│  ┌─────────────┐                           │                             │
│  │ dim_dept    │◄──────────────────────────┤                             │
│  │             │                           │                             │
│  └─────────────┘                           │                             │
│  ┌─────────────┐                           │                             │
│  │ dim_geo     │◄──────────────────────────┤                             │
│  │             │                           │                             │
│  └─────────────┘                           │                             │
│  ┌─────────────┐                           │                             │
│  │ dim_status  │◄──────────────────────────┘                             │
│  │             │                                                        │
│  └─────────────┘                                                        │
│                                                                          │
│  Dashboard Marts:                                                       │
│  ┌────────────────────────┐  ┌────────────────────────┐                │
│  │ mart_operational_     │  │ mart_sla_equity_       │                │
│  │   dashboard           │  │   dashboard            │                │
│  │                      │  │                       │                │
│  │ - Daily metrics       │  │ - Weekly metrics       │                │
│  │ - Current backlog     │  │ - SLA compliance      │                │
│  │ - At-risk requests    │  │ - Equity analysis      │                │
│  └────────────────────────┘  └────────────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## dbt Models Structure

```
transform/
├── models/
│   ├── staging/
│   │   ├── _sources.yml         # Source definitions
│   │   ├── _stg_models.yml      # Schema tests
│   │   └── stg_service_requests.sql
│   │
│   ├── intermediate/
│   │   ├── _int_models.yml
│   │   └── int_sla_performance.sql
│   │
│   └── marts/
│       ├── _marts_models.yml
│       ├── core/                    # Kimball dimensions and facts
│       │   ├── dim_date.sql
│       │   ├── dim_request_type.sql
│       │   ├── dim_department.sql
│       │   ├── dim_geography.sql
│       │   ├── dim_status.sql
│       │   └── fct_service_requests.sql
│       │
│       └── operations/              # Business-specific marts
│           └── mart_sla_equity_dashboard.sql
│
├── seeds/                           # Static reference data
│   ├── sla_targets.csv
│   └── community_areas.csv
│
├── macros/                          # Reusable SQL logic
│   └── soft_delete/
│       └── apply_soft_delete.sql
│
├── dbt_project.yml                  # Project configuration
├── profiles.yml                     # Connection profiles
└── packages.yml                     # External dependencies
```

---

## Dashboard Stories

### Dashboard 1: Operational Dashboard

**Audience**: Operations team, department managers, ward aldermen

**Frequency**: Daily updates, real-time monitoring

**Story This Tells:**

**"What's happening right now?"**

This dashboard provides immediate visibility into the current state of service delivery. Operations managers can quickly identify:
- Which departments are experiencing backlogs
- Which neighborhoods have the oldest unresolved requests
- Whether the city is keeping pace with new requests vs. resolutions
- Which specific requests are at risk of breaching SLA commitments

**Key Visualizations:**

1. **Current Backlog by Department** — Horizontal bar chart showing open requests, with SLA-at-risk requests highlighted
2. **Oldest Open Requests by Community Area** — Map or bar chart showing neighborhoods with aging requests
3. **24-Hour Activity** — Line chart comparing new requests vs. resolved requests over time
4. **At-Risk Requests List** — Table of open requests approaching SLA breach, sorted by days to breach
5. **Response Time Trend** — 7-day moving average of time to close

**Sample Insight:**
> "The Streets & Sanitation department currently has 847 open requests, with 23% at risk of breaching their 3-day SLA target. The largest backlog is in Ward 25 with 67 open pothole requests."

### Dashboard 2: SLA & Equity Dashboard

**Audience**: Executive leadership, department heads, auditors

**Frequency**: Weekly updates, monthly trend analysis

**Story This Tells:**

**"Are we meeting our commitments, and is service equitable across the city?"**

This dashboard provides strategic intelligence on performance, accountability, and fairness. Executives can answer:
- Are departments meeting their published response time commitments?
- Which request types breach SLA most often, and why?
- Are some community areas receiving systematically slower service than others?
- How has SLA compliance trended over the past 6 months?
- What would we need to report for the annual audit?

**Key Visualizations:**

1. **SLA Compliance Rate (Last 30 Days)** — Gauge chart showing % of requests meeting SLA
2. **SLA Compliance by Department** — Bar chart with department-level breakdown
3. **SLA Compliance by Request Type** — Table showing breach rates by type
4. **Equity Heat Map** — Map of Chicago showing equity index by community area (green = fast, red = slow)
5. **Trending Breach Types** — Line chart showing which request types have increasing breach rates
6. **Compliance Trend (6 Months)** — Monthly SLA compliance over time

**Sample Insight:**
> "Overall SLA compliance is 87% for the month, up from 82% last month. The Parks department leads at 94%, while Transportation lags at 79%. Community Area 4 (Lincoln Square) has an equity index of 1.4—40% slower than citywide average for graffiti removal requests."

---

## Data Quality & Governance

### Source Data Guarantees

| Guarantee | Column(s) | Enforcement |
|-----------|-----------|-------------|
| Not Null | `service_request_number`, `created_date`, `last_modified_date` | `stg_service_requests` tests |
| Unique | `service_request_number` | `stg_service_requests` tests |
| Valid Range | `created_month` (1–12) | `stg_service_requests` tests |
| Referential | SLA targets for all `sr_type` | Seed data + default fallback |

### Business Rules (Documented in Tests)

| Rule ID | Description | Model |
|---------|-------------|-------|
| BR-001 | Duplicates excluded from SLA calculations | `int_sla_performance` |
| BR-002 | Legacy records (pre-2018) excluded from analysis | `stg_service_requests` |
| BR-003 | SLA targets from seed, 14-day default for unknown types | `int_sla_performance` |
| BR-004 | Resolution time = calendar days (not business days) | `int_sla_performance` |
| BR-005 | Equity index = area_avg / citywide_avg by type and period | `int_sla_performance` |
| BR-006 | Open requests: `days_open` uses current date | `fct_service_requests` |
| BR-007 | SLA at-risk = (days_open > sla_target_days) for open requests | `fct_service_requests` |

---

## Performance Considerations

### Partitioning

The source table is partitioned by:
- `created_year` (integer)
- `created_month` (integer)

This enables efficient queries like:
```sql
-- Only scans 2024 data
WHERE created_year = 2024

-- Only scans Q4 2024
WHERE created_year = 2024 AND created_month IN (10, 11, 12)
```

### Incremental Loading

The daily flow uses `last_modified_date` watermarking to fetch only changed records:
```sql
WHERE last_modified_date > @last_successful_run
```

This minimizes data movement and processing time for daily updates.

### Materialization Strategy

| Layer | Materialization | Rationale |
|-------|----------------|-----------|
| Staging | View | No storage cost, always current |
| Intermediate | Table | Avoids recomputing SLA logic |
| Dimensions | Table | Small reference data, fast joins |
| Fact | Table | Large fact table, aggregations |
| Dashboard Marts | Table | Pre-aggregated for fast dashboard queries |

---

## Appendix: Reference Data

### SLA Target Categories

| Category | SLA Target | Example Request Types |
|----------|-------------|----------------------|
| Critical | 1 day | Rodent control, immediate hazards |
| High | 3 days | Pothole repair, tree emergency |
| Medium | 7 days | Graffiti removal, street light out |
| Low | 14 days | Abandoned vehicle, bulky item pickup |

### Chicago Geographic Hierarchy

```
Chicago City
├── Community Areas (77)
│   └── Multiple Wards (0–50)
├── Wards (50)
│   └── Multiple Community Areas
└── ZIP Codes
    └── Crosses ward/community area boundaries
```

---

## Future Enhancements

1. **Predictive Analytics**: ML model to predict SLA breach risk
2. **Seasonal Adjustments**: Account for weather patterns affecting service times
3. **Voice of Citizen**: Integrate citizen feedback and satisfaction surveys
4. **Resource Optimization**: Model staffing needs based on historical demand patterns
5. **Real-time Alerts**: Push notifications for critical SLA breaches
