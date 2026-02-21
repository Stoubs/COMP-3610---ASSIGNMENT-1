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
            f"After applying the date, hour, and payment filters, {top_zone} comes out on top with {top_count:,} pickups "
            f"({top_share:.2f}% of the trips you’re currently looking at). "
            f"By the time you get down to the 10th zone ({tenth_zone}), the count drops to {tenth_count:,} trips "
            f"({tenth_share:.2f}%). "
            f"That gap is a sign that demand is not evenly distributed across the city—there are a few zones that dominate pickup activity, "
            f"which is exactly what you’d expect in a taxi dataset where hotspots (busy commercial areas, transit hubs, nightlife zones) generate a lot more trips than quiet residential areas."
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
            f"This line chart looks at how trip cost changes across the day. Each point is the average fare for all trips that started in that hour, "
            f"so you’re seeing a time-of-day pattern rather than individual rides. "
            f"In the filtered data, the highest average fare shows up around {int(peak.pickup_hour)}:00 at ${float(peak.avg_fare):.2f}, "
            f"and the lowest average fare is around {int(low.pickup_hour)}:00 at ${float(low.avg_fare):.2f}. "
            f"A practical interpretation is that the “mix” of trips changes over the day—at some hours, trips tend to be longer, or travel through heavier traffic, "
            f"or include more airport-type rides, which pushes the average fare up. At other hours, the rides are more local and cheaper on average."
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
            f"This histogram is showing the shape of the distance distribution, which is one of the first things you’d check in a taxi dataset. "
            f"Most of the mass is packed toward the left, meaning short trips are very common and long trips are comparatively rare. "
            f"Using the median helps summarize that skew: the median distance is {median_dist} miles, so 50% of trips are at or below {median_dist} miles. "
            f"When you zoom in on very short rides, {short_share}% of trips are 2 miles or less, which lines up with the high bars near the smallest distances. "
            f"In analytics terms, this is a right-skewed distribution, and it’s a reminder that averages can be pulled upward by a smaller number of long rides, "
            f"so looking at medians and percent splits gives a clearer picture of what a “typical” trip looks like."
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
                f"This pie chart is a quick way to see rider behavior at the payment step. It’s counting trips by payment_type and turning that into percentages. "
                f"In the filtered data, {top_p.payment_name} is clearly the main payment method at {top_pct:.2f}% of trips, "
                f"and {second.payment_name} is next at {second_pct:.2f}%. "
                f"Because the same filters apply here, this chart is useful for comparing scenarios rather than claiming a single global truth about all taxi trips."
            )
        else:
            st.write(
                f"This pie chart shows that {top_p.payment_name} makes up {top_pct:.2f}% of trips in the filtered data. "
                f"With the current filters, every trip falls into the same payment category, so you don’t see a split. "
                f"If you widen the filters (for example, expand the date or hour range), you would normally expect more payment categories to appear and the pie to diversify."
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
            f"This heatmap is basically the weekly rhythm of taxi demand compressed into one view. "
            f"Each cell is a count of trips for a specific day-of-week and hour-of-day combination, and the color intensity increases as the trip count increases. "
            f"In the filtered data, the single busiest cell is {peak_cell.day_of_week} around {int(peak_cell.pickup_hour)}:00, with {int(peak_cell.trip_count):,} trips in that one slot. "
            f"When you add up the cells across hours, {busiest_day} ends up being the busiest overall day in the current view with {busiest_day_trips:,} trips. "
            f"The main value here is that you can immediately spot peak windows (where demand clusters) versus quieter periods, and then compare how those patterns shift when you change filters. "
            f"This is the kind of visualization that helps you move from “there are lots of trips” to “there are predictable, repeatable peaks in time that drive most of the activity.”"
        )

