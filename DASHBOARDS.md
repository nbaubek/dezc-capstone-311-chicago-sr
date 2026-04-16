# Chicago 311 Dashboard Guide

Two dashboards built in Looker Studio connect directly to the `chicago_311_sr_analytics` BigQuery dataset.

---

## Dashboard 1: Operations Triage

https://datastudio.google.com/reporting/11103a58-9563-4e3b-9578-140f5cc7c866

**Purpose:** Real-time view of the open backlog for dispatch and triage workflows.
**Refresh:** Auto-refresh every 15 minutes.
**Audience:** Operations staff doing daily triage.

### Layout

```
┌─────────────────────────────────────────────────────────────────┐
│  311 OPERATIONS DASHBOARD                    [Dept ▾] [CA ▾]  │
│  Live open backlog · Auto-refreshes every 15 min              │
├──────────────┬──────────────┬──────────────┬─────────────────┤
│  TOTAL OPEN   │  OVERDUE     │  DUE TODAY   │  AT RISK        │
│  12,847      │  423 🔴      │  891 🟡      │  2,104 🟠       │
├──────────────┴──────────────┴──────────────┴─────────────────┤
│                                                                 │
│  TICKETS BY PRIORITY (table)          TICKETS BY AGE (bar)     │
│  ┌─────────────────────────┐        ┌────────────────────┐     │
│  │ Priority │ Count │ AvgDays│        │ 0-3d  ████████   │     │
│  │ Critical │  234  │  8.2d  │        │ 4-7d  ████       │     │
│  │ High    │ 1,203 │ 12.4d  │        │ 8-14d ██          │     │
│  │ Medium  │ 8,441 │  5.1d  │        │ 15+d  █           │     │
│  │ Low     │ 2,969 │  3.7d   │        └────────────────────┘     │
│  └─────────────────────────┘                                      │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  TRIAGE STATUS BY DEPARTMENT (pivot table)                      │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Dept           │ 1-Overdue │ 2-DueToday │ 3-AtRisk │...│ │
│  │ Streets & San  │    187    │     342    │    891   │   │ │
│  │ Water Mgmt     │     92    │     201    │    445   │   │ │
│  │ CDOT           │     56    │     134    │    312   │   │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  OPEN TICKETS (detail table — clickable, sortable)              │
│  ┌──────────┬────────────┬───────────────────┬───────┬──────┐ │
│  │ SR Number│ Created     │ Request Type       │ CA    │ Days │ │
│  │ SR24-... │ 2024-03-01 │ Pothole Repair     │ 25    │  12  │ │
│  │ SR24-... │ 2024-03-02 │ Graffiti Removal   │  8    │  11  │ │
│  └──────────┴────────────┴───────────────────┴───────┴──────┘ │
│  ◀ Showing top 50 of 12,847 ▶  [Download CSV]                │
└─────────────────────────────────────────────────────────────────┘
```

### Controls

| Control | Type | Column in `mart_operational` |
|---------|------|------------------------------|
| Department | Dropdown (multi-select) | `department_name` |
| Community Area | Dropdown (multi-select) | `community_area_name` |
| Priority | Dropdown | `priority_level` |
| Date Range | Date range picker | `created_date` |

### Components

**Scorecard — Total Open**
```
SELECT COUNT(*) FROM mart_operational
WHERE status_name != 'Completed'
-- or: WHERE closed_date IS NULL (if column is added to mart later)
```

**Scorecard — Overdue** (triage_status = '1 - Overdue')
```
SELECT COUNT(*) FROM mart_operational
WHERE triage_status = '1 - Overdue'
```

**Scorecard — Due Today** (triage_status = '2 - Due Today')
```
SELECT COUNT(*) FROM mart_operational
WHERE triage_status = '2 - Due Today'
```

**Scorecard — At Risk** (triage_status = '3 - At Risk')
```
SELECT COUNT(*) FROM mart_operational
WHERE triage_status = '3 - At Risk'
```

**Table — Tickets by Priority**
```
SELECT
  priority_level,
  COUNT(*) as ticket_count,
  ROUND(AVG(days_open), 1) as avg_days_open
FROM mart_operational
GROUP BY priority_level
ORDER BY
  CASE priority_level
    WHEN 'Critical' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
  END
```

**Bar Chart — Tickets by Age Bucket**
```
SELECT
  CASE
    WHEN days_open BETWEEN 0 AND 3  THEN '0-3 days'
    WHEN days_open BETWEEN 4 AND 7  THEN '4-7 days'
    WHEN days_open BETWEEN 8 AND 14 THEN '8-14 days'
    ELSE '15+ days'
  END as age_bucket,
  COUNT(*) as ticket_count
FROM mart_operational
GROUP BY age_bucket
ORDER BY
  CASE age_bucket
    WHEN '0-3 days' THEN 1
    WHEN '4-7 days' THEN 2
    WHEN '8-14 days' THEN 3
    WHEN '15+ days' THEN 4
  END
```

**Pivot Table — Triage Status by Department**
```
SELECT
  department_name,
  SUM(CASE WHEN triage_status = '1 - Overdue' THEN 1 ELSE 0 END) as overdue,
  SUM(CASE WHEN triage_status = '2 - Due Today' THEN 1 ELSE 0 END) as due_today,
  SUM(CASE WHEN triage_status = '3 - At Risk' THEN 1 ELSE 0 END) as at_risk,
  SUM(CASE WHEN triage_status = '4 - On Track' THEN 1 ELSE 0 END) as on_track,
  COUNT(*) as total
FROM mart_operational
GROUP BY department_name
ORDER BY overdue DESC
```

**Table — Open Tickets Detail**
```
SELECT
  service_request_number,
  created_date,
  request_type_name,
  community_area_name,
  department_name,
  days_open,
  days_until_breach,
  triage_status
FROM mart_operational
ORDER BY days_until_breach ASC
LIMIT 200
```

---

## Dashboard 2: SLA & Equity Performance

**Purpose:** Aggregated SLA performance and equity analysis for executives and compliance reporting.
**Refresh:** Daily.
**Audience:** City executives, compliance auditors.

### Layout

```
┌──────────────────────────────────────────────────────────────────┐
│  SLA & EQUITY PERFORMANCE           [Year ▾] [Dept ▾] [CA ▾]  │
│  2024 · Citywide view                                        │
├────────────────┬────────────────┬────────────────┬────────────────┤
│ COMPLETION RATE│ AVG RESOLUTION │  BREACH RATE    │  EQUITY INDEX  │
│    87.3%      │   8.4 days    │    12.7%       │    1.08       │
│  ▲ +2.1% vs   │  ▼ -0.3d vs  │  ▲ +1.2pp vs  │  Some areas   │
│  last month    │  last month   │  last month   │  underperforming│
├────────────────┴────────────────┴────────────────┴────────────────┤
│                                                                  │
│  EQUITY HEATMAP (community area × request type)                │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              │ Pothole │ Graffiti │ Vacant │ Street   │   │
│  │  25 Lincoln  │  🔴 1.4  │  🟡 1.1  │  --    │  🔴 1.3  │   │
│  │   4 Uptown   │  🟡 1.2  │  🟢 0.9  │  🔴 1.5 │  🟡 1.1  │   │
│  │  43 North   │  🟢 0.9  │  🟢 0.8  │  🔴 1.3 │  🟡 1.0  │   │
│  │  ...        │   ...    │   ...    │  ...   │   ...    │   │
│  │  Legend: 🟢 <0.9 | 🟡 0.9-1.1 | 🔴 >1.1                   │   │
│  └──────────────────────────────────────────────────────────┘   │
│  Click cell → drill into that community area + request type     │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  BREACH RATE BY DEPARTMENT (horizontal bar, sorted)             │
│  Streets & San  ████████████████████████░░░░░  18.2%           │
│  Water Mgmt     ██████████████████░░░░░░░░░  14.1%           │
│  CDOT           ████████████░░░░░░░░░░░░░░  11.3%           │
│  Buildings      ██████████░░░░░░░░░░░░░░░   9.7%            │
│  ...                                                         │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│  EQUITY STATUS SUMMARY                                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ Status            │ # Areas │ % of City │ Avg Res Days    │ │
│  │ Severely Undersv  │    12   │   15.6%   │    15.2 days    │ │
│  │ Underserved       │    23   │   29.9%   │    11.8 days    │ │
│  │ Equitable         │    28   │   36.4%   │     7.9 days    │ │
│  │ Over-served       │    14   │   18.2%   │     5.1 days    │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

### Controls

| Control | Type | Column in `mart_sla_performance` |
|---------|------|----------------------------------|
| Year | Dropdown | `closed_date` (or a Year parameter) |
| Department | Dropdown (multi-select) | `request_type_name` (join to dim_department) |
| Community Area | Dropdown (multi-select) | `community_area_name` |
| Request Type | Dropdown (multi-select) | `request_type_name` |

### Components

**Scorecard — Completion Rate**
```
SELECT
  ROUND(
    COUNT(CASE WHEN equity_index IS NOT NULL THEN 1 END) * 100.0 /
    COUNT(*), 1
  ) as completion_rate
FROM mart_sla_performance
```

**Scorecard — Avg Resolution Days**
```
SELECT ROUND(AVG(area_avg_resolution_days), 1)
FROM mart_sla_performance
```

**Scorecard — Breach Rate**
```
SELECT
  ROUND(SUM(area_breached_requests) * 100.0 / SUM(area_total_requests), 1)
FROM mart_sla_performance
```

**Scorecard — Citywide Equity Index**
```
SELECT ROUND(AVG(equity_index), 2)
FROM mart_sla_performance
WHERE area_total_requests >= 10
```

**Scorecard — Delta vs Prior Period**
Use Looker Studio comparison mode on the above scorecards, comparing current selection vs same period prior year.

**Heatmap — Equity Index by Community Area × Request Type**
Looker Studio doesn't have a native heatmap, so use a **pivot table** with conditional formatting:

```
SELECT
  community_area_name,
  request_type_name,
  area_total_requests,
  ROUND(area_avg_resolution_days, 1) as avg_days,
  ROUND(equity_index, 2) as equity_index,
  equity_status
FROM mart_sla_performance
WHERE area_total_requests >= 10
ORDER BY community_area_name, equity_index DESC
```

Apply conditional formatting on `equity_index`:
- 🟢 Green: ≤ 0.90
- 🟡 Yellow: 0.91 – 1.10
- 🔴 Red: > 1.10

**Bar Chart — Breach Rate by Department**
```
SELECT
  dep.department_name,
  ROUND(SUM(f.area_breached_requests) * 100.0 /
    SUM(f.area_total_requests), 1) as breach_rate
FROM mart_sla_performance f
LEFT JOIN {{ ref('dim_request_type') }} req ON f.request_type_id = req.request_type_id
LEFT JOIN {{ ref('dim_department') }} dep ON req.owner_department = dep.raw_department_name
GROUP BY dep.department_name
ORDER BY breach_rate DESC
```
*Note: This requires joining mart_sla_performance back to dim_request_type and dim_department, or adding department_name directly to mart_sla_performance.*

**Table — Equity Status Summary**
```
SELECT
  equity_status,
  COUNT(DISTINCT community_area_name) as num_areas,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as pct_of_city,
  ROUND(AVG(area_avg_resolution_days), 1) as avg_resolution_days
FROM mart_sla_performance
WHERE area_total_requests >= 10
GROUP BY equity_status
ORDER BY
  CASE equity_status
    WHEN 'Severely Underserved (25%+ Slower)' THEN 1
    WHEN 'Underserved (10-24% Slower)' THEN 2
    WHEN 'Equitable (Average)' THEN 3
    WHEN 'Over-served (Faster than Average)' THEN 4
  END
```

---

## BigQuery Dataset

Both dashboards connect to: **`chicago_311_sr_analytics`** (the dbt target dataset, configured in `transform/profiles.yml`).

## Tables to Use

| Table | Used In |
|-------|---------|
| `mart_operational` | Dashboard 1 |
| `mart_sla_performance` | Dashboard 2 |
| `dim_request_type` | Joining for department-level breakdowns |
| `dim_department` | Joining for department-level breakdowns |
| `fct_service_requests` | Detail drill-downs |
| `fct_sla_performance` | Time-series trend analysis |

---

## Looker Studio Setup Steps

1. Go to [lookerstudio.google.com](https://lookerstudio.google.com)
2. Click **+ Create** → **Report**
3. Click **Add data** → select **BigQuery**
4. Under **My Projects**, select your GCP project → select **`chicago_311_sr_analytics`** dataset → choose the table or write a custom query
5. For custom queries (recommended for filtered views), use the **Enter custom query** option and paste the SQL from the component sections above
6. After adding the data source, drag **charts** from the toolbar onto the canvas
7. For each chart, click the data source and configure:
   - **Dimension:** the column to group by
   - **Metric:** the aggregation (COUNT, SUM, AVG, etc.)
8. Add **filter controls**:
   - Click **Add a control** in the toolbar
   - Choose **Dropdown**, **Date range**, or **Text input**
   - Link the control to the relevant dimension in your charts
9. Set **interactions**:
   - Click a chart → **Interactions** panel → enable **Drill down** or **Cross-filtering** so clicking a value filters the whole dashboard
10. Add a **theme** (top right → **Theme**):
    - Use **City of Chicago** brand colors: dark blue `#003366`, red `#C60C30`, yellow `#FFD100`, white `#FFFFFF`
11. Add a **title page** as the first tab with the dashboard name, last refresh time, and a brief description of what it shows
12. Set **auto-refresh**:
    - **Resource** → **Manage added data sources** → click the data source → **Refresh settings** → set to **Every 15 minutes** for Dashboard 1 (operations), **Daily** for Dashboard 2
13. **Share** the report:
    - Click **Share** → set viewing permissions
    - For production use, consider publishing to the web or restricting to specific Google Workspace accounts

---

## Conditional Formatting

Apply these rules to make the dashboards visually interpretable at a glance:

| Dashboard | Column | Rule | Color |
|----------|--------|------|-------|
| 1 — Operations | `days_until_breach` | < 0 | Red |
| 1 — Operations | `days_until_breach` | 0 – 2 | Orange |
| 1 — Operations | `days_until_breach` | > 2 | Green |
| 1 — Operations | `triage_status` | '1 - Overdue' | Red |
| 1 — Operations | `triage_status` | '2 - Due Today' | Yellow |
| 2 — SLA | `equity_index` | > 1.25 | Red |
| 2 — SLA | `equity_index` | 1.10 – 1.25 | Orange |
| 2 — SLA | `equity_index` | 0.90 – 1.10 | Yellow |
| 2 — SLA | `equity_index` | < 0.90 | Green |
| 2 — SLA | `breach_rate` | > 15% | Red |
| 2 — SLA | `breach_rate` | 10 – 15% | Orange |
| 2 — SLA | `breach_rate` | < 10% | Green |
