# -----------------------------
# Imports
# -----------------------------
# duckdb: runs SQL queries on the taxi database
# pandas: used for minor dataframe manipulation
# streamlit: builds the web dashboard
# plotly: generates interactive charts
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.io as pio


# -----------------------------
# Streamlit + Plot Styling Setup
# -----------------------------
# Use dark theme for all Plotly charts
pio.templates.default = "plotly_dark"

# Configure Streamlit page layout
st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")


# -----------------------------
# Database Connection
# -----------------------------
# Connect to local DuckDB file containing taxi data
con = duckdb.connect("taxi.duckdb")


# -----------------------------
# Page Title + Intro
# -----------------------------
st.title("NYC Yellow Taxi Dashboard")
st.write(
    "This dashboard summarizes NYC Yellow Taxi trips using key metrics and 5 required visualizations. "
    "Use the filters in the sidebar to explore patterns by date, hour, and payment type."
)


# -----------------------------
# Get Minimum + Maximum Dates
# -----------------------------
# This determines the full range of available pickup dates
min_date, max_date = con.execute("""
SELECT MIN(DATE(tpep_pickup_datetime)), MAX(DATE(tpep_pickup_datetime))
FROM trips
""").fetchone()


# -----------------------------
# Sidebar Filters
# -----------------------------
st.sidebar.header("Filters")

# Date range filter
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

# Hour filter (0–23)
hour_range = st.sidebar.slider("Pickup hour range", 0, 23, (0, 23))


# -----------------------------
# Payment Type Mapping
# -----------------------------
# TLC payment type codes translated to readable labels
payment_labels = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

# Fetch available payment types from dataset
df_pay = con.execute("""
SELECT DISTINCT payment_type
FROM trips
ORDER BY payment_type
""").fetchdf()

# Convert numeric codes into readable dropdown options
pay_options = [payment_labels.get(p, f"Other ({p})") for p in df_pay["payment_type"].tolist()]

# Sidebar multiselect
selected_payments = st.sidebar.multiselect(
    "Payment types",
    options=pay_options,
    default=pay_options
)


# -----------------------------
# Convert Selected Labels Back to Codes
# -----------------------------
# Map selected payment names back to numeric codes for SQL filtering
selected_codes = [code for code, name in payment_labels.items() if name in selected_payments]

# Handle "Other (x)" cases safely
for opt in selected_payments:
    if opt.startswith("Other (") and opt.endswith(")"):
        raw = opt.replace("Other (", "").replace(")", "")
        if raw.isdigit():
            selected_codes.append(int(raw))


# -----------------------------
# Build WHERE Clause for Filtering
# -----------------------------
start_date, end_date = date_range[0], date_range[1]
h1, h2 = hour_range

# Convert selected payment codes into SQL format
codes_sql = "(" + ",".join(map(str, selected_codes)) + ")" if selected_codes else "(NULL)"

# Final filter clause reused across all queries
where_clause = f"""
WHERE DATE(tpep_pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
  AND EXTRACT('hour' FROM tpep_pickup_datetime) BETWEEN {h1} AND {h2}
  AND payment_type IN {codes_sql}
"""


# -----------------------------
# Metrics Section
# -----------------------------
# Try to use total_amount (preferred)
# If missing, fallback to fare + tip
try:
    metrics = con.execute(f"""
    SELECT
      COUNT(*) AS total_trips,
      ROUND(AVG(fare_amount), 2) AS avg_fare,
      ROUND(SUM(total_amount), 2) AS total_revenue,
      ROUND(AVG(trip_distance), 2) AS avg_distance,
      ROUND(AVG(DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime)), 2) AS avg_duration_min
    FROM trips
    {where_clause}
    """).fetchdf().iloc[0]
except:
    metrics = con.execute(f"""
    SELECT
      COUNT(*) AS total_trips,
      ROUND(AVG(fare_amount), 2) AS avg_fare,
      ROUND(SUM(fare_amount + COALESCE(tip_amount, 0)), 2) AS total_revenue,
      ROUND(AVG(trip_distance), 2) AS avg_distance,
      ROUND(AVG(DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime)), 2) AS avg_duration_min
    FROM trips
    {where_clause}
    """).fetchdf().iloc[0]


# Display metrics in 5 columns
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total trips", f"{int(metrics.total_trips):,}")
c2.metric("Average fare", f"${metrics.avg_fare:.2f}")
c3.metric("Total revenue", f"${metrics.total_revenue:.2f}")
c4.metric("Avg trip distance", f"{metrics.avg_distance:.2f} mi")
c5.metric("Avg trip duration", f"{metrics.avg_duration_min:.2f} min")

st.divider()


# -----------------------------
# Tabs Section
# -----------------------------
tab1, tab2 = st.tabs(["Visualizations", "Notes"])