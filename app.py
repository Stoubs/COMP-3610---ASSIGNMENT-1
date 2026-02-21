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

# ---------------- App config ----------------
pio.templates.default = "plotly_dark"
st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")

st.title("NYC Yellow Taxi Dashboard")
st.write(
    "This dashboard summarizes NYC Yellow Taxi trips using key metrics and 5 required visualizations. "
    "Use the filters in the sidebar to explore patterns by date, hour, and payment type."
)

# ---------------- Data sources (cloud-safe) ----------------
TRIPS_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
)
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# ---------------- Helpers ----------------
payment_labels = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}


@st.cache_data(show_spinner=True)
def get_date_bounds():
    q = f"""
    SELECT
      MIN(CAST(tpep_pickup_datetime AS DATE)) AS min_date,
      MAX(CAST(tpep_pickup_datetime AS DATE)) AS max_date
    FROM read_parquet('{TRIPS_URL}')
    """
    return duckdb.query(q).fetchone()


@st.cache_data(show_spinner=True)
def get_distinct_payment_types():
    q = f"""
    SELECT DISTINCT payment_type
    FROM read_parquet('{TRIPS_URL}')
    ORDER BY payment_type
    """
    return duckdb.query(q).fetchdf()


def build_where_clause(start_date, end_date, h1, h2, selected_codes):
    codes_sql = (
        "(" + ",".join(map(str, selected_codes)) + ")" if selected_codes else "(NULL)"
    )
    return f"""
    WHERE CAST(tpep_pickup_datetime AS DATE) BETWEEN '{start_date}' AND '{end_date}'
      AND EXTRACT('hour' FROM tpep_pickup_datetime) BETWEEN {h1} AND {h2}
      AND payment_type IN {codes_sql}
    """


# ---------------- Sidebar filters ----------------
min_date, max_date = get_date_bounds()

st.sidebar.header("Filters")
date_range = st.sidebar.date_input(
    "Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date
)

hour_range = st.sidebar.slider("Pickup hour range", 0, 23, (0, 23))

df_pay = get_distinct_payment_types()
pay_options = [
    payment_labels.get(p, f"Other ({p})") for p in df_pay["payment_type"].tolist()
]

selected_payments = st.sidebar.multiselect(
    "Payment types", options=pay_options, default=pay_options
)

# map selected payment names back to codes
selected_codes = [
    code for code, name in payment_labels.items() if name in selected_payments
]
for opt in selected_payments:
    if opt.startswith("Other (") and opt.endswith(")"):
        raw = opt.replace("Other (", "").replace(")", "")
        if raw.isdigit():
            selected_codes.append(int(raw))

start_date, end_date = date_range[0], date_range[1]
h1, h2 = hour_range

where_clause = build_where_clause(start_date, end_date, h1, h2, selected_codes)


# ---------------- Metrics ----------------
@st.cache_data(show_spinner=False)
def get_metrics(where_clause: str):
    q = f"""
    SELECT
      COUNT(*) AS total_trips,
      ROUND(AVG(fare_amount), 2) AS avg_fare,
      ROUND(SUM(total_amount), 2) AS total_revenue,
      ROUND(AVG(trip_distance), 2) AS avg_distance,
      ROUND(AVG(DATE_DIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime)), 2) AS avg_duration_min
    FROM read_parquet('{TRIPS_URL}')
    {where_clause}
    """
    return duckdb.query(q).fetchdf().iloc[0]


metrics = get_metrics(where_clause)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total trips", f"{int(metrics.total_trips):,}")
c2.metric("Average fare", f"${metrics.avg_fare:.2f}")
c3.metric("Total revenue", f"${metrics.total_revenue:.2f}")
c4.metric("Avg trip distance", f"{metrics.avg_distance:.2f} mi")
c5.metric("Avg trip duration", f"{metrics.avg_duration_min:.2f} min")

st.divider()

tab1, tab2 = st.tabs(["Visualizations", "Notes"])

with tab1:
    # r) Bar: Top 10 pickup zones by trip count (join lookup)
    @st.cache_data(show_spinner=False)
    def top_pickup_zones(where_clause: str):
        q = f"""
        WITH trips AS (
          SELECT * FROM read_parquet('{TRIPS_URL}')
          {where_clause}
        ),
        zones AS (
          SELECT * FROM read_csv_auto('{ZONES_URL}')
        )
        SELECT
          z.Zone AS pickup_zone,
          COUNT(*) AS trip_count
        FROM trips t
        JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY 1
        ORDER BY trip_count DESC
        LIMIT 10
        """
        return duckdb.query(q).fetchdf()

    df_r = top_pickup_zones(where_clause)

    fig_r = px.bar(
        df_r,
        x="pickup_zone",
        y="trip_count",
        title="Top 10 Pickup Zones by Trip Count",
    )
    fig_r.update_layout(margin=dict(l=40, r=20, t=60, b=160))
    fig_r.update_xaxes(tickangle=45)
    st.plotly_chart(fig_r, use_container_width=True)

    if len(df_r) > 0 and metrics.total_trips > 0:
        total_trips = int(metrics.total_trips)
        top_zone = df_r.iloc[0]["pickup_zone"]
        top_count = int(df_r.iloc[0]["trip_count"])
        top_share = (top_count / total_trips) * 100
        top10_share = (int(df_r["trip_count"].sum()) / total_trips) * 100

        st.write(
            f"""
**Insight:**  
In this filtered slice, **{top_zone}** is the busiest pickup zone with **{top_count:,} trips** (**{top_share:.2f}%** of all trips).  
The top 10 zones together account for **{top10_share:.2f}%**, showing demand is concentrated in a small set of areas.
"""
        )

    st.divider()

    # s) Line: Average fare by hour
    @st.cache_data(show_spinner=False)
    def avg_fare_by_hour(where_clause: str):
        q = f"""
        SELECT
          EXTRACT('hour' FROM tpep_pickup_datetime) AS pickup_hour,
          ROUND(AVG(fare_amount), 2) AS avg_fare
        FROM read_parquet('{TRIPS_URL}')
        {where_clause}
        GROUP BY 1
        ORDER BY pickup_hour
        """
        return duckdb.query(q).fetchdf()

    df_s = avg_fare_by_hour(where_clause)
    fig_s = px.line(
        df_s,
        x="pickup_hour",
        y="avg_fare",
        markers=True,
        title="Average Fare by Hour of Day",
    )
    st.plotly_chart(fig_s, use_container_width=True)

    if len(df_s) > 0:
        peak = df_s.loc[df_s["avg_fare"].idxmax()]
        low = df_s.loc[df_s["avg_fare"].idxmin()]
        st.write(
            f"""
**Insight:**  
Average fares vary by hour. The highest average fare occurs around **{int(peak.pickup_hour)}:00** (**${float(peak.avg_fare):.2f}**), while the lowest occurs around **{int(low.pickup_hour)}:00** (**${float(low.avg_fare):.2f}**).  
This suggests different trip types (short vs long, commute vs leisure) dominate at different times of day.
"""
        )

    st.divider()

    # t) Histogram: Trip distance distribution
    @st.cache_data(show_spinner=False)
    def trip_distances(where_clause: str):
        q = f"""
        SELECT trip_distance
        FROM read_parquet('{TRIPS_URL}')
        {where_clause}
          AND trip_distance > 0 AND trip_distance <= 50
        """
        return duckdb.query(q).fetchdf()

    df_t = trip_distances(where_clause)
    fig_t = px.histogram(
        df_t,
        x="trip_distance",
        nbins=40,
        title="Distribution of Trip Distances (0–50 miles)",
    )
    st.plotly_chart(fig_t, use_container_width=True)

    if len(df_t) > 0:
        median_dist = duckdb.query(
            f"""
            SELECT ROUND(MEDIAN(trip_distance), 2)
            FROM read_parquet('{TRIPS_URL}')
            {where_clause}
              AND trip_distance > 0 AND trip_distance <= 50
            """
        ).fetchone()[0]

        short_share = duckdb.query(
            f"""
            SELECT ROUND(100.0 * SUM(CASE WHEN trip_distance <= 2 THEN 1 ELSE 0 END) / COUNT(*), 2)
            FROM read_parquet('{TRIPS_URL}')
            {where_clause}
              AND trip_distance > 0 AND trip_distance <= 50
            """
        ).fetchone()[0]

        st.write(
            f"""
**Insight:**  
Trip distances skew short: the median distance is **{median_dist} miles**, and **{short_share}%** of trips are **2 miles or less** (within the 0–50 mile range).  
This indicates most rides are quick local trips rather than long journeys.
"""
        )

    st.divider()

    # u) Pie/bar: Payment breakdown
    @st.cache_data(show_spinner=False)
    def payment_breakdown(where_clause: str):
        q = f"""
        SELECT payment_type, COUNT(*) AS trip_count
        FROM read_parquet('{TRIPS_URL}')
        {where_clause}
        GROUP BY 1
        ORDER BY trip_count DESC
        """
        return duckdb.query(q).fetchdf()

    df_u = payment_breakdown(where_clause)
    df_u["payment_name"] = df_u["payment_type"].map(payment_labels).fillna("Other")

    fig_u = px.pie(
        df_u, names="payment_name", values="trip_count", title="Payment Type Breakdown"
    )
    fig_u.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_u, use_container_width=True)

    if len(df_u) > 0:
        top = df_u.sort_values("trip_count", ascending=False).iloc[0]
        top_pct = (top.trip_count / df_u.trip_count.sum()) * 100
        st.write(
            f"""
**Insight:**  
**{top.payment_name}** is the most common payment method in this filtered view, making up **{top_pct:.2f}%** of trips.  
That dominance suggests rider behavior is strongly skewed toward one payment style rather than evenly split.
"""
        )

    st.divider()

    # v) Heatmap: Trips by day of week and hour
    @st.cache_data(show_spinner=False)
    def trips_by_dow_hour(where_clause: str):
        q = f"""
        SELECT
          STRFTIME(tpep_pickup_datetime, '%A') AS day_of_week,
          EXTRACT('hour' FROM tpep_pickup_datetime) AS pickup_hour,
          COUNT(*) AS trip_count,
          EXTRACT('dow' FROM tpep_pickup_datetime) AS dow_num
        FROM read_parquet('{TRIPS_URL}')
        {where_clause}
        GROUP BY 1, 2, 4
        ORDER BY dow_num, pickup_hour
        """
        return duckdb.query(q).fetchdf()

    df_v = trips_by_dow_hour(where_clause)
    dow_order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    df_v["day_of_week"] = pd.Categorical(
        df_v["day_of_week"], categories=dow_order, ordered=True
    )

    fig_v = px.density_heatmap(
        df_v,
        x="pickup_hour",
        y="day_of_week",
        z="trip_count",
        histfunc="sum",
        title="Trips by Day of Week and Hour",
    )
    st.plotly_chart(fig_v, use_container_width=True)

    if len(df_v) > 0:
        peak_cell = df_v.loc[df_v["trip_count"].idxmax()]
        st.write(
            f"""
**Insight:**  
The busiest window is **{peak_cell.day_of_week}** around **{int(peak_cell.pickup_hour)}:00**, where trip counts peak relative to other day/hour combinations.  
This indicates taxi demand follows a weekly rhythm (not just a daily one), with certain days having stronger peak-hour surges.
"""
        )

with tab2:
    st.write(
        """
**Notes**
- This deployed version reads the NYC TLC parquet file and zone lookup CSV directly (no local database file required).
- This approach avoids file-locking issues and works on Streamlit Community Cloud.
"""
    )
