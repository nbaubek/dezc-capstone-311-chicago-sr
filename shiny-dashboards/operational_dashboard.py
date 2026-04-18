from shiny import App, ui, render, reactive
from google.cloud import bigquery
import polars as pl
import plotly.express as px

# --------------------------------------------------------
# 1. DATA INGESTION (Runs once on startup)
# --------------------------------------------------------
client = bigquery.Client()

# Pull the flat mart directly from BigQuery. 
# We use .to_arrow() which is lightning fast, then wrap it in Polars.
query = """
    SELECT 
        service_request_number, created_date, community_area_name, 
        department_name, request_type_name, priority_level, 
        days_open, days_until_breach, triage_status
    FROM `project-4b4006dc-66f0-46ce-a26.marts.mart_operational`
"""
arrow_table = client.query(query).to_arrow()
df_raw = pl.from_arrow(arrow_table)

# Build the Age Buckets in Polars natively
df = df_raw.with_columns(
    pl.when(pl.col("days_open") <= 3).then(pl.lit("0-3 days"))
    .when(pl.col("days_open") <= 7).then(pl.lit("4-7 days"))
    .when(pl.col("days_open") <= 14).then(pl.lit("8-14 days"))
    .otherwise(pl.lit("15+ days")).alias("age_bucket")
)

# --------------------------------------------------------
# 2. UI FRONTEND (Matches DASHBOARDS.md Layout)
# --------------------------------------------------------
app_ui = ui.page_fluid(
    ui.h2("Chicago 311: Operations Dashboard"),
    ui.p("Live open backlog · Auto-refreshes via BigQuery"),
    
    # Controls
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_select("dept", "Department", choices=["All"] + df["department_name"].drop_nulls().unique().to_list()),
            ui.input_select("priority", "Priority", choices=["All", "Critical", "High", "Medium", "Low"]),
        ),
        
        # KPI Scorecards
        ui.layout_columns(
            ui.value_box("Total Open", ui.output_text("kpi_total")),
            ui.value_box("Overdue 🔴", ui.output_text("kpi_overdue")),
            ui.value_box("Due Today 🟡", ui.output_text("kpi_due_today")),
            ui.value_box("At Risk 🟠", ui.output_text("kpi_at_risk")),
        ),
        
        # Row 1: Charts
        ui.layout_columns(
            ui.card(ui.h5("Tickets by Priority"), ui.output_data_frame("table_priority")),
            ui.card(ui.h5("Tickets by Age"), ui.output_ui("chart_age")),
        ),
        
        # Row 2: Pivot & Details
        ui.card(ui.h5("Triage Status by Department"), ui.output_data_frame("pivot_dept")),
        ui.card(ui.h5("Open Tickets Detail (Top 200)"), ui.output_data_frame("table_detail"))
    )
)

# --------------------------------------------------------
# 3. SERVER BACKEND (Polars Data Manipulation)
# --------------------------------------------------------
def server(input, output, session):
    
    # -- Reactive Data Filter --
    @reactive.Calc
    def filtered_df():
        filtered = df
        if input.dept() != "All":
            filtered = filtered.filter(pl.col("department_name") == input.dept())
        if input.priority() != "All":
            filtered = filtered.filter(pl.col("priority_level") == input.priority())
        return filtered

    # -- KPIs --
    @render.text
    def kpi_total():
        return f"{filtered_df().height:,}"

    @render.text
    def kpi_overdue():
        val = filtered_df().filter(pl.col("triage_status") == "1 - Overdue").height
        return f"{val:,}"

    @render.text
    def kpi_due_today():
        val = filtered_df().filter(pl.col("triage_status") == "2 - Due Today").height
        return f"{val:,}"

    @render.text
    def kpi_at_risk():
        val = filtered_df().filter(pl.col("triage_status") == "3 - At Risk").height
        return f"{val:,}"

    # -- Tickets by Priority Table --
    @render.data_frame
    def table_priority():
        res = (
            filtered_df()
            .group_by("priority_level")
            .agg([
                pl.count().alias("Count"),
                pl.col("days_open").mean().round(1).alias("Avg Days")
            ])
            .sort("priority_level") # You can add custom sorting here
        )
        return res.to_pandas() # Shiny tables currently expect Pandas objects natively

    # -- Tickets by Age Bar Chart (Plotly) --
    @render.ui
    def chart_age():
        chart_data = (
            filtered_df()
            .group_by("age_bucket")
            .agg(pl.count().alias("ticket_count"))
            .sort("age_bucket")
        ).to_pandas()
        
        fig = px.bar(chart_data, x="age_bucket", y="ticket_count", color_discrete_sequence=["#003366"])
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300)
        return ui.HTML(fig.to_html(include_plotlyjs="require", full_html=False))

    # -- Triage Pivot Table --
    @render.data_frame
    def pivot_dept():
        res = (
            filtered_df()
            .pivot(
                values="service_request_number", 
                index="department_name", 
                columns="triage_status", 
                aggregate_function="count"
            )
            .fill_null(0)
        )
        return res.to_pandas()

    # -- Detail Table --
    @render.data_frame
    def table_detail():
        res = (
            filtered_df()
            .select(["service_request_number", "created_date", "request_type_name", "department_name", "days_open", "triage_status"])
            .sort("days_open", descending=True)
            .head(200)
        )
        return res.to_pandas()

app = App(app_ui, server)