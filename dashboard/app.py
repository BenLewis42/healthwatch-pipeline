"""
Streamlit dashboard for CDC surveillance data visualization.

Displays key metrics, trends, and data quality status with interactive components.
"""

import streamlit as st
import duckdb
import plotly.express as px
import pandas as pd
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="CDC Surveillance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Title
st.title("📊 CDC Surveillance Data Dashboard")
st.markdown("Real-time monitoring of disease surveillance metrics")

# Sidebar
st.sidebar.header("Dashboard Controls")

# Connect to database and load data with fallback names
def _try_queries(conn, queries):
    for q in queries:
        try:
            df = conn.execute(q).df()
            if not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()

try:
    conn = duckdb.connect("data/warehouse.duckdb")

    covid_queries = [
        "SELECT * FROM raw.covid_surveillance ORDER BY date DESC LIMIT 10000",
        "SELECT * FROM analytics_staging.stg_covid_surveillance ORDER BY date DESC LIMIT 10000",
        "SELECT * FROM raw.places_county ORDER BY extracted_at DESC LIMIT 10000",
    ]

    flu_queries = [
        "SELECT * FROM raw.flu_surveillance ORDER BY week DESC LIMIT 10000",
        "SELECT * FROM analytics_staging.stg_flu_surveillance ORDER BY week DESC LIMIT 10000",
    ]

    covid_df = _try_queries(conn, covid_queries)
    flu_df = _try_queries(conn, flu_queries)

    if covid_df.empty and flu_df.empty:
        st.warning(
            "No surveillance tables found in the database. Did you run the pipeline?"
        )

except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

# Main metrics row with defensive column handling
col1, col2, col3, col4 = st.columns(4)

def _first_column(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# COVID metrics
cases_col = _first_column(covid_df, [
    "cases",
    "case_count",
    "confirmed_cases",
    "covid_cases",
    "total_cases",
    "cases_total",
])
deaths_col = _first_column(covid_df, ["deaths", "death_count", "deaths_total"])
date_col = _first_column(covid_df, ["date", "report_date", "extracted_at"])

if not covid_df.empty and cases_col:
    try:
        total_covid_cases = covid_df[cases_col].astype(float).sum()
    except Exception:
        total_covid_cases = 0
    col1.metric("Total COVID Cases", f"{int(total_covid_cases):,}")
else:
    col1.metric("Total COVID Cases", "N/A")

if not covid_df.empty and deaths_col:
    try:
        total_covid_deaths = covid_df[deaths_col].astype(float).sum()
    except Exception:
        total_covid_deaths = 0
    col2.metric("Total COVID Deaths", f"{int(total_covid_deaths):,}")
else:
    col2.metric("Total COVID Deaths", "N/A")

# Flu / ILI metric
ili_col = _first_column(flu_df, ["weighted_ili", "ili_percent", "weighted_ili_percent"])
if not flu_df.empty and ili_col:
    try:
        avg_weighted_ili = flu_df[ili_col].astype(float).mean()
        col3.metric("Avg Weighted ILI", f"{avg_weighted_ili:.2f}%")
    except Exception:
        col3.metric("Avg Weighted ILI", "N/A")
else:
    col3.metric("Avg Weighted ILI", "N/A")

col4.metric("Dashboard Status", "✓ Operational")

# Tabs for different sections
tab1, tab2, tab3 = st.tabs(["📈 Trends", "🗺️ Geographic", "✓ Data Quality"])

# Tab 1: Trends
with tab1:
    st.subheader("COVID Cases Over Time")

    if covid_df.empty:
        st.info("No COVID data available.")
    elif date_col and cases_col:
        # Sort by date for proper visualization
        covid_sorted = covid_df.sort_values(date_col)

        fig = px.line(
            covid_sorted,
            x=date_col,
            y=cases_col,
            title="Daily COVID Cases",
            labels={cases_col: "Number of Cases", date_col: "Date"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Summary stats
        st.metric("Latest Date", str(covid_sorted[date_col].max()))
        try:
            st.metric("Latest Daily Cases", int(covid_sorted[cases_col].iloc[-1]))
        except Exception:
            pass
    else:
        st.info("Time-series metric not available for this dataset. Showing available sample rows.")
        st.dataframe(covid_df.head(10))

# Tab 2: Geographic
with tab2:
    st.subheader("Cases by State")

    state_col = _first_column(covid_df, ["state", "state_code", "state_name", "statedesc", "stateabbr"])

    if covid_df.empty or not state_col:
        st.info("State-level data not available for this dataset.")
    else:
        if cases_col:
            state_cases = covid_df.groupby(state_col)[cases_col].sum().reset_index()
            metric_col = cases_col
        else:
            # fallback to population or row counts
            pop_col = _first_column(covid_df, ["total_population", "population", "total_populaton"])
            if pop_col:
                state_cases = covid_df.groupby(state_col)[pop_col].sum().reset_index()
                metric_col = pop_col
            else:
                state_cases = covid_df.groupby(state_col).size().reset_index(name="count")
                metric_col = "count"

        state_cases = state_cases.sort_values(metric_col, ascending=False).head(20)

        fig = px.bar(
            state_cases,
            x=state_col,
            y=metric_col,
            title="Top 20 States by Metric",
            labels={metric_col: "Total", state_col: "State"},
        )
        st.plotly_chart(fig, use_container_width=True)

# Tab 3: Data Quality
with tab3:
    st.subheader("Data Quality Status")

    try:
        # Load quality report if it exists
        import json
        from pathlib import Path

        report_path = Path("data_quality/report.json")
        if report_path.exists():
            with open(report_path) as f:
                report = json.load(f)

            # Display summary
            col1, col2 = st.columns(2)
            col1.metric("Checks Passed", report["summary"]["passed"])
            col2.metric("Checks Failed", report["summary"]["failed"])

            # Display individual checks
            for check in report["checks"]:
                status_color = "🟢" if check["status"] == "PASS" else "🔴"
                st.write(f"{status_color} **{check['name']}**: {check['status']}")

        else:
            st.info("No quality report available yet. Run the pipeline first.")

    except Exception as e:
        st.warning(f"Could not load quality report: {e}")

# Footer
st.divider()
st.caption(
    f"Dashboard updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
    f"Data source: CDC Surveillance APIs"
)
