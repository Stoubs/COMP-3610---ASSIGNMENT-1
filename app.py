import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.io as pio
import os
import urllib.request
import streamlit as st

con = duckdb.connect("taxi.duckdb")

TRIPS_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
)
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

#  use a sample of the parquet instead of the full dataset
SAMPLE_PERCENT = 1
TRIPS_SOURCE = f"read_parquet('{TRIPS_URL}') USING SAMPLE {SAMPLE_PERCENT} PERCENT"

os.makedirs("data/raw", exist_ok=True)

trips_path = "data/raw/yellow_tripdata_2024-01.parquet"
zones_path = "data/raw/taxi_zone_lookup.csv"


def download_if_missing(url, path):
    if not os.path.exists(path):
        st.write(f"Downloading {os.path.basename(path)}...")
        urllib.request.urlretrieve(url, path)


download_if_missing(TRIPS_URL, trips_path)
download_if_missing(ZONES_URL, zones_path)

has_trips = con.execute("""
SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'trips'
""").fetchone()[0]

has_zones = con.execute("""
SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'zones'
""").fetchone()[0]

if has_trips == 0:
    con.execute(f"CREATE TABLE trips AS SELECT * FROM read_parquet('{trips_path}')")

if has_zones == 0:
    con.execute(f"CREATE TABLE zones AS SELECT * FROM read_csv_auto('{zones_path}')")

pio.templates.default = "plotly_dark"

st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")


# ---------- Page Title + Intro 
st.title("NYC Yellow Taxi Dashboard")
st.write(
    "This dashboard summarizes NYC Yellow Taxi trips using key metrics and 5 required visualizations. "
    "Use the filters in the sidebar to explore patterns by date, hour, and payment type."
)

# ---------- Sidebar Filters (10) ----------
date_bounds = con.execute("""
SELECT
  MIN(DATE(tpep_pickup_datetime)) AS min_date,
  MAX(DATE(tpep_pickup_datetime)) AS max_date
FROM trips
""").fetchone()

min_date, max_date = date_bounds[0], date_bounds[1]

st.sidebar.header("Filters")
date_range = st.sidebar.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
hour_range = st.sidebar.slider("Pickup hour range", 0, 23, (0, 23))

df_pay = con.execute("""
SELECT DISTINCT payment_type
FROM trips
ORDER BY payment_type
""").fetchdf()

payment_labels = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

pay_options = []
for p in df_pay["payment_type"].tolist():
    pay_options.append(payment_labels.get(p, f"Other ({p})"))

selected_payments = st.sidebar.multiselect(
    "Payment types",
    options=pay_options,
    default=pay_options
)

# map selected payment names back to codes 
selected_codes = []
for code, name in payment_labels.items():
    if name in selected_payments:
        selected_codes.append(code)
for opt in selected_payments:
    if opt.startswith("Other (") and opt.endswith(")"):
        try:
            selected_codes.append(int(opt.replace("Other (", "").replace(")", "")))
        except:
            pass

# build WHERE clause 
start_date = date_range[0]
end_date = date_range[1]
h1, h2 = hour_range

codes_sql = "(" + ",".join(str(c) for c in selected_codes) + ")" if len(selected_codes) > 0 else "(NULL)"

where_clause = f"""
WHERE DATE(tpep_pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
  AND EXTRACT('hour' FROM tpep_pickup_datetime) BETWEEN {h1} AND {h2}
  AND payment_type IN {codes_sql}
"""

# ---------- Metrics 
# total_amount exists in TLC data usually; fallback is fare+tip
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

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total trips", f"{int(metrics.total_trips):,}")
c2.metric("Average fare", f"${metrics.avg_fare:.2f}")
c3.metric("Total revenue", f"${metrics.total_revenue:.2f}")
c4.metric("Avg trip distance", f"{metrics.avg_distance:.2f} mi")
c5.metric("Avg trip duration", f"{metrics.avg_duration_min:.2f} min")

st.divider()

# ---------- Dashboard Structure 
tab1, tab2 = st.tabs(["Visualizations", "Notes"])

with tab1:
    # r) Bar: Top 10 pickup zones
    df_r = con.execute(f"""
    SELECT
      z.Zone AS pickup_zone,
      COUNT(*) AS trip_count
    FROM trips t
    JOIN zones z ON t.PULocationID = z.LocationID
    {where_clause}
    GROUP BY 1
    ORDER BY trip_count DESC
    LIMIT 10
    """).fetchdf()

    fig_r = px.bar(
        df_r,
        x="pickup_zone",
        y="trip_count",
        title="Top 10 Pickup Zones by Trip Count",
        color="trip_count",
        color_continuous_scale=["#1C1C1C", "#8B0000", "#FF4500", "#FFA500"]
    )
    fig_r.update_layout(margin=dict(l=40, r=20, t=60, b=160))
    fig_r.update_xaxes(tickangle=45)
    st.plotly_chart(fig_r, use_container_width=True)

    if len(df_r) > 0 and metrics.total_trips > 0:
        total_trips = int(metrics.total_trips)

        top_zone = df_r.iloc[0]["pickup_zone"]
        top_count = int(df_r.iloc[0]["trip_count"])
        top_share = (top_count / total_trips) * 100

        tenth_zone = df_r.iloc[-1]["pickup_zone"]
        tenth_count = int(df_r.iloc[-1]["trip_count"])
        tenth_share = (tenth_count / total_trips) * 100

        st.write(
            f"In the filtered data, the busiest pickup zone is {top_zone} with {top_count:,} trips "
            f"({top_share:.2f}% of all filtered trips). "
            f"The 10th zone in the top-10 list is {tenth_zone} with {tenth_count:,} trips "
            f"({tenth_share:.2f}%), which shows how trip demand is concentrated in a small set of pickup locations."
        )

    st.divider()

    # s) Line: Average fare by hour
    df_s = con.execute(f"""
    SELECT
      EXTRACT('hour' FROM tpep_pickup_datetime) AS pickup_hour,
      ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM trips
    {where_clause}
    GROUP BY 1
    ORDER BY pickup_hour
    """).fetchdf()

    fig_s = px.line(df_s, x="pickup_hour", y="avg_fare", markers=True,
                    title="Average Fare by Hour of Day")
    fig_s.update_traces(line=dict(color="#FF4500", width=3), marker=dict(color="#FF4500"))
    st.plotly_chart(fig_s, use_container_width=True)

    if len(df_s) > 0:
        peak = df_s.loc[df_s["avg_fare"].idxmax()]
        low = df_s.loc[df_s["avg_fare"].idxmin()]

        st.write(
            f"The line chart shows how the average fare changes across pickup hours in the filtered data. "
            f"The highest average fare occurs around {int(peak.pickup_hour)}:00 at ${float(peak.avg_fare):.2f}, "
            f"while the lowest average fare occurs around {int(low.pickup_hour)}:00 at ${float(low.avg_fare):.2f}. "
            f"This indicates that certain hours tend to have higher-cost trips compared to others."
        )

    st.divider()

    # t) Histogram: Trip distance distribution
    df_t = con.execute(f"""
    SELECT trip_distance
    FROM trips
    {where_clause}
      AND trip_distance > 0 AND trip_distance <= 50
    """).fetchdf()

    fig_t = px.histogram(df_t, x="trip_distance", nbins=40,
                         title="Distribution of Trip Distances (0–50 miles)",
                         color_discrete_sequence=["#B22222"])
    st.plotly_chart(fig_t, use_container_width=True)

    if len(df_t) > 0:
        median_dist = con.execute(f"""
        SELECT ROUND(MEDIAN(trip_distance), 2)
        FROM trips
        {where_clause}
          AND trip_distance > 0 AND trip_distance <= 50
        """).fetchone()[0]

        short_share = con.execute(f"""
        SELECT ROUND(100.0 * SUM(CASE WHEN trip_distance <= 2 THEN 1 ELSE 0 END) / COUNT(*), 2)
        FROM trips
        {where_clause}
          AND trip_distance > 0 AND trip_distance <= 50
        """).fetchone()[0]

        st.write(
            f"The histogram shows that trip distances are mostly concentrated at shorter values in the filtered data. "
            f"The median trip distance is {median_dist} miles, meaning half of trips are shorter than {median_dist} miles. "
            f"About {short_share}% of trips are 2 miles or less, which matches the high frequency of short-distance bins."
        )

    st.divider()

    # u) Pie: Payment type breakdown (red/orange/black)
    df_u = con.execute(f"""
    SELECT payment_type, COUNT(*) AS trip_count
    FROM trips
    {where_clause}
    GROUP BY 1
    ORDER BY trip_count DESC
    """).fetchdf()

    df_u["payment_name"] = df_u["payment_type"].map(payment_labels).fillna("Other")

    fig_u = px.pie(
        df_u,
        names="payment_name",
        values="trip_count",
        title="Payment Type Breakdown",
        color_discrete_sequence=["#FF3B30", "#FF9500", "#1C1C1C", "#555555", "#888888", "#B22222"]
    )
    fig_u.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_u, use_container_width=True)

    if len(df_u) > 0:
        df_u_sorted = df_u.sort_values("trip_count", ascending=False).reset_index(drop=True)
        top_p = df_u_sorted.iloc[0]
        top_pct = (top_p.trip_count / df_u_sorted.trip_count.sum()) * 100

        if len(df_u_sorted) > 1:
            second = df_u_sorted.iloc[1]
            second_pct = (second.trip_count / df_u_sorted.trip_count.sum()) * 100
            st.write(
                f"The pie chart breaks down trips by payment method in the filtered data. "
                f"{top_p.payment_name} is the most common method at {top_pct:.2f}% of trips, "
                f"followed by {second.payment_name} at {second_pct:.2f}%. "
                f"This shows which payment methods dominate under the current filters."
            )
        else:
            st.write(
                f"The pie chart shows that {top_p.payment_name} accounts for {top_pct:.2f}% of trips in the filtered data. "
                f"Only one payment category appears under the current filters, so the distribution is fully concentrated in that method."
            )

    st.divider()

    # v) Heatmap: Trips by day of week and hour
    df_v = con.execute(f"""
    SELECT
      STRFTIME(tpep_pickup_datetime, '%A') AS day_of_week,
      EXTRACT('hour' FROM tpep_pickup_datetime) AS pickup_hour,
      COUNT(*) AS trip_count,
      EXTRACT('dow' FROM tpep_pickup_datetime) AS dow_num
    FROM trips
    {where_clause}
    GROUP BY 1, 2, 4
    ORDER BY dow_num, pickup_hour
    """).fetchdf()

    dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    df_v["day_of_week"] = pd.Categorical(df_v["day_of_week"], categories=dow_order, ordered=True)

    red_orange_scale = [
        [0.0, "#1a0000"],
        [0.3, "#4d0000"],
        [0.6, "#b22222"],
        [0.8, "#ff4500"],
        [1.0, "#ffa500"],
    ]

    fig_v = px.density_heatmap(
        df_v,
        x="pickup_hour",
        y="day_of_week",
        z="trip_count",
        histfunc="sum",
        title="Trips by Day of Week and Hour",
        color_continuous_scale=red_orange_scale
    )
    st.plotly_chart(fig_v, use_container_width=True)

    if len(df_v) > 0:
        peak_cell = df_v.loc[df_v["trip_count"].idxmax()]

        day_totals = df_v.groupby("day_of_week", observed=True)["trip_count"].sum().reset_index()
        busiest_day_row = day_totals.loc[day_totals["trip_count"].idxmax()]
        busiest_day = busiest_day_row["day_of_week"]
        busiest_day_trips = int(busiest_day_row["trip_count"])

        st.write(
            f"The heatmap shows how pickup volume varies by day of week and hour in the filtered data. "
            f"The single busiest day-hour cell is {peak_cell.day_of_week} around {int(peak_cell.pickup_hour)}:00 "
            f"with {int(peak_cell.trip_count):,} trips. "
            f"Across all hours shown, {busiest_day} has the highest total pickups with {busiest_day_trips:,} trips."
        )

with tab2:
    st.write(
        "This prototype confirms the required metrics and exactly five required visualizations render correctly. "
        "Filters update all charts and metrics based on the selected date range, hour range, and payment types."
    )