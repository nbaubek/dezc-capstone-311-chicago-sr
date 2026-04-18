from shiny import App, ui, render, reactive
from google.cloud import bigquery
import polars as pl
import plotly.express as px

# --------------------------------------------------------
# 1. DATA INGESTION (Runs once on startup)
# --------------------------------------------------------
client = bigquery.Client()

query = """
    SELECT
        community_area_name, city_side, request_type_name, priority_level,
        department_name, bureau, area_total_requests, area_breached_requests,
        area_breach_rate, area_avg_resolution_days, citywide_avg_resolution_days,
        equity_index, equity_status
    FROM `project-4b4006dc-66f0-46ce-a26.marts.mart_sla_performance`
"""
arrow_table = client.query(query).to_arrow()
df_raw = pl.from_arrow(arrow_table)

# Equity buckets for charts
df = df_raw.with_columns(
    pl.when(pl.col("equity_index") >= 1.25).then(pl.lit("Severely Underserved"))
    .when(pl.col("equity_index") >= 1.10).then(pl.lit("Underserved"))
    .when(pl.col("equity_index") <= 0.90).then(pl.lit("Over-served"))
    .otherwise(pl.lit("Equitable")).alias("equity_bucket")
)

# --------------------------------------------------------
# 2. UI FRONTEND (Matches DASHBOARDS.md Layout)
# --------------------------------------------------------
app_ui = ui.page_fluid(
    ui.h2("Chicago 311: SLA & Equity Dashboard"),
    ui.p("Resolution equity by community area · Department comparison"),

    # Controls
    ui.layout_sidebar(
        ui.sidebar(
            ui.input_select("dept", "Department", choices=["All"] + df["department_name"].drop_nulls().unique().to_list()),
            ui.input_select("equity_status", "Equity Status", choices=["All", "Severely Underserved (25%+ Slower)", "Underserved (10-24% Slower)", "Equitable (Average)", "Over-served (Faster than Average)"]),
        ),

        # KPI Scorecards
        ui.layout_columns(
            ui.value_box("Total Areas", ui.output_text("kpi_total_areas")),
            ui.value_box("Avg Breach Rate", ui.output_text("kpi_avg_breach")),
            ui.value_box("Severely Underserved", ui.output_text("kpi_severe")),
            ui.value_box("Avg Resolution Days", ui.output_text("kpi_avg_days")),
        ),

        # Row 1: Charts
        ui.layout_columns(
            ui.card(ui.h5("Equity Status Distribution"), ui.output_ui("chart_equity_status")),
            ui.card(ui.h5("Top 10 Areas by Breach Rate"), ui.output_ui("chart_top_breach")),
        ),

        # Row 2: Department Performance & Detail
        ui.layout_columns(
            ui.card(ui.h5("Avg Breach Rate by Department"), ui.output_ui("chart_dept")),
            ui.card(ui.h5("Resolution Days vs Citywide Avg"), ui.output_ui("chart_resolution")),
        ),

        # Row 3: Detail Table
        ui.card(ui.h5("Area Equity Detail (Top 200)"), ui.output_data_frame("table_detail"))
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
        if input.equity_status() != "All":
            filtered = filtered.filter(pl.col("equity_status") == input.equity_status())
        return filtered

    # -- KPIs --
    @render.text
    def kpi_total_areas():
        return f"{filtered_df().height:,}"

    @render.text
    def kpi_avg_breach():
        val = filtered_df().select(pl.col("area_breach_rate").mean()).to_numpy()[0, 0]
        return f"{val:.1%}"

    @render.text
    def kpi_severe():
        val = filtered_df().filter(pl.col("equity_status") == "Severely Underserved (25%+ Slower)").height
        return f"{val:,}"

    @render.text
    def kpi_avg_days():
        val = filtered_df().select(pl.col("area_avg_resolution_days").mean()).to_numpy()[0, 0]
        return f"{val:.1f}"

    # -- Equity Status Pie Chart --
    @render.ui
    def chart_equity_status():
        chart_data = (
            filtered_df()
            .group_by("equity_status")
            .agg(pl.sum("area_total_requests").alias("total_requests"))
            .sort("equity_status")
        ).to_pandas()

        fig = px.pie(
            chart_data,
            names="equity_status",
            values="total_requests",
            color_discrete_sequence=["#cc0000", "#ff9900", "#003366", "#006600"],
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300)
        return ui.HTML(fig.to_html(include_plotlyjs="require", full_html=False))

    # -- Top 10 Areas by Breach Rate --
    @render.ui
    def chart_top_breach():
        chart_data = (
            filtered_df()
            .sort("area_breach_rate", descending=True)
            .head(10)
            .select(["community_area_name", "area_breach_rate"])
        ).to_pandas()

        fig = px.bar(
            chart_data,
            x="community_area_name",
            y="area_breach_rate",
            color_discrete_sequence=["#003366"],
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=300,
            xaxis_tickangle=-45,
        )
        return ui.HTML(fig.to_html(include_plotlyjs="require", full_html=False))

    # -- Department Breach Rate Comparison --
    @render.ui
    def chart_dept():
        chart_data = (
            filtered_df()
            .group_by("department_name")
            .agg([
                pl.sum("area_breached_requests").alias("breached"),
                pl.sum("area_total_requests").alias("total"),
            ])
            .with_columns((pl.col("breached") / pl.col("total")).alias("breach_rate"))
            .sort("breach_rate", descending=True)
        ).to_pandas()

        fig = px.bar(
            chart_data,
            x="department_name",
            y="breach_rate",
            color_discrete_sequence=["#003366"],
        )
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
            height=300,
            xaxis_tickangle=-45,
        )
        return ui.HTML(fig.to_html(include_plotlyjs="require", full_html=False))

    # -- Resolution Days vs Citywide Average --
    @render.ui
    def chart_resolution():
        chart_data = (
            filtered_df()
            .select(["community_area_name", "area_avg_resolution_days", "citywide_avg_resolution_days"])
            .sort("area_avg_resolution_days", descending=True)
            .head(20)
        ).to_pandas()

        fig = px.scatter(
            chart_data,
            x="area_avg_resolution_days",
            y="citywide_avg_resolution_days",
            text="community_area_name",
            labels={
                "area_avg_resolution_days": "Area Avg Resolution Days",
                "citywide_avg_resolution_days": "Citywide Avg Resolution Days",
            },
        )
        fig.update_traces(textposition="top center", marker=dict(size=10))
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300)
        return ui.HTML(fig.to_html(include_plotlyjs="require", full_html=False))

    # -- Detail Table --
    @render.data_frame
    def table_detail():
        res = (
            filtered_df()
            .select([
                "community_area_name", "city_side", "request_type_name",
                "department_name", "area_total_requests", "area_breach_rate",
                "area_avg_resolution_days", "equity_index", "equity_status"
            ])
            .sort("area_breach_rate", descending=True)
            .head(200)
        )
        return res.to_pandas()

app = App(app_ui, server)
