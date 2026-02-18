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

# Connect to database
try:
    conn = duckdb.connect("data/warehouse.duckdb")

    # Load data
    covid_df = conn.execute(
        "SELECT * FROM raw.covid_surveillance ORDER BY date DESC LIMIT 10000"
    ).df()
    flu_df = conn.execute(
        "SELECT * FROM raw.flu_surveillance ORDER BY week DESC LIMIT 10000"
    ).df()

except Exception as e:
    st.error(f"Failed to connect to database: {e}")
    st.stop()

# Main metrics row
col1, col2, col3, col4 = st.columns(4)

if not covid_df.empty:
    total_covid_cases = covid_df["cases"].sum()
    total_covid_deaths = covid_df["deaths"].sum()
    col1.metric("Total COVID Cases", f"{int(total_covid_cases):,}")
    col2.metric("Total COVID Deaths", f"{int(total_covid_deaths):,}")

if not flu_df.empty:
    avg_weighted_ili = flu_df["weighted_ili"].mean()
    col3.metric("Avg Weighted ILI", f"{avg_weighted_ili:.2f}%")

col4.metric("Dashboard Status", "✓ Operational")

# Tabs for different sections
tab1, tab2, tab3 = st.tabs(["📈 Trends", "🗺️ Geographic", "✓ Data Quality"])

# Tab 1: Trends
with tab1:
    st.subheader("COVID Cases Over Time")

    if not covid_df.empty and "date" in covid_df.columns:
        # Sort by date for proper visualization
        covid_sorted = covid_df.sort_values("date")

        fig = px.line(
            covid_sorted,
            x="date",
            y="cases",
            title="Daily COVID Cases",
            labels={"cases": "Number of Cases", "date": "Date"},
        )
        st.plotly_chart(fig, use_container_width=True)

        # Summary stats
        st.metric("Latest Date", str(covid_sorted["date"].max()))
        st.metric("Latest Daily Cases", int(covid_sorted["cases"].iloc[-1]))

# Tab 2: Geographic
with tab2:
    st.subheader("Cases by State")

    if not covid_df.empty and "state" in covid_df.columns:
        state_cases = covid_df.groupby("state")["cases"].sum().reset_index()
        state_cases = state_cases.sort_values("cases", ascending=False).head(20)

        fig = px.bar(
            state_cases,
            x="state",
            y="cases",
            title="Top 20 States by Cases",
            labels={"cases": "Total Cases", "state": "State"},
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
