import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.io as pio

pio.templates.default = "plotly_dark"
st.set_page_config(page_title="NYC Taxi Dashboard", layout="wide")

con = duckdb.connect("taxi.duckdb")

st.title("NYC Yellow Taxi Dashboard")
st.write(
    "This dashboard summarizes NYC Yellow Taxi trips using key metrics and 5 required visualizations. "
    "Use the filters in the sidebar to explore patterns by date, hour, and payment type."
)

# --- Date bounds
min_date, max_date = con.execute("""
SELECT MIN(DATE(tpep_pickup_datetime)), MAX(DATE(tpep_pickup_datetime))
FROM trips
""").fetchone()

# --- Sidebar filters
st.sidebar.header("Filters")
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)
hour_range = st.sidebar.slider("Pickup hour range", 0, 23, (0, 23))

payment_labels = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}

df_pay = con.execute("""
SELECT DISTINCT payment_type
FROM trips
ORDER BY payment_type
""").fetchdf()

pay_options = [payment_labels.get(p, f"Other ({p})") for p in df_pay["payment_type"].tolist()]

selected_payments = st.sidebar.multiselect(
    "Payment types",
    options=pay_options,
    default=pay_options
)

# map selected payment names back to codes
selected_codes = [code for code, name in payment_labels.items() if name in selected_payments]

for opt in selected_payments:
    if opt.startswith("Other (") and opt.endswith(")"):
        raw = opt.replace("Other (", "").replace(")", "")
        if raw.isdigit():
            selected_codes.append(int(raw))

# --- WHERE clause
start_date, end_date = date_range[0], date_range[1]
h1, h2 = hour_range

codes_sql = "(" + ",".join(map(str, selected_codes)) + ")" if selected_codes else "(NULL)"

where_clause = f"""
WHERE DATE(tpep_pickup_datetime) BETWEEN '{start_date}' AND '{end_date}'
  AND EXTRACT('hour' FROM tpep_pickup_datetime) BETWEEN {h1} AND {h2}
  AND payment_type IN {codes_sql}
"""

# ---------- Metrics ----------
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

        if len(df_r) > 1:
            second_zone = df_r.iloc[1]["pickup_zone"]
            second_count = int(df_r.iloc[1]["trip_count"])
            second_share = (second_count / total_trips) * 100
            gap = top_share - second_share
        else:
            second_zone, second_count, second_share, gap = None, None, None, None

        top10_share = (int(df_r["trip_count"].sum()) / total_trips) * 100

        st.write(
            f"""
            **Insight:**  
            In the filtered data, pickups are not spread out evenly across the city. **{top_zone}** comes out on top with **{top_count:,} trips**, which is **{top_share:.2f}%** of everything in this slice. This means a noticeable chunk of trips are coming from one main area instead of being shared across many zones.

            The top 10 pickup zones together account for **{top10_share:.2f}%** of all filtered trips, so demand is clearly concentrated.{" " if second_zone is None else f"Also, **{top_zone}** is ahead of **{second_zone}** by about **{gap:.2f} percentage points**, so it’s not just narrowly leading — it’s pulling away."}
            """
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

    fig_s = px.line(
        df_s,
        x="pickup_hour",
        y="avg_fare",
        markers=True,
        title="Average Fare by Hour of Day"
    )
    fig_s.update_traces(line=dict(color="#FF4500", width=3), marker=dict(color="#FF4500"))
    st.plotly_chart(fig_s, use_container_width=True)

    if len(df_s) > 0:
        peak = df_s.loc[df_s["avg_fare"].idxmax()]
        low = df_s.loc[df_s["avg_fare"].idxmin()]

        peak_hour = int(peak.pickup_hour)
        peak_fare = float(peak.avg_fare)
        low_hour = int(low.pickup_hour)
        low_fare = float(low.avg_fare)

        delta = peak_fare - low_fare
        pct_change = (delta / low_fare) * 100 if low_fare != 0 else 0

        st.write(
            f"""
            **Insight:**  
            Average fare changes a lot depending on the hour. The highest point shows up around **{peak_hour}:00** at **${peak_fare:.2f}**, while the lowest is around **{low_hour}:00** at **${low_fare:.2f}**. That’s a gap of **${delta:.2f}** (about **{pct_change:.1f}%** higher than the low point).

            What this suggests is that the trips happening during the peak hour tend to be “bigger” rides in some way — either longer distances, longer time on the road, or just more expensive trips overall. Meanwhile, the low hour looks like a period where rides are generally cheaper, which usually lines up with shorter or more local trips.
            """
        )

    st.divider()

    # t) Histogram: Trip distance distribution
    df_t = con.execute(f"""
    SELECT trip_distance
    FROM trips
    {where_clause}
      AND trip_distance > 0 AND trip_distance <= 50
    """).fetchdf()

    fig_t = px.histogram(
        df_t,
        x="trip_distance",
        nbins=40,
        title="Distribution of Trip Distances (0–50 miles)",
        color_discrete_sequence=["#B22222"]
    )
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

        long_share = con.execute(f"""
        SELECT ROUND(100.0 * SUM(CASE WHEN trip_distance >= 10 THEN 1 ELSE 0 END) / COUNT(*), 2)
        FROM trips
        {where_clause}
          AND trip_distance > 0 AND trip_distance <= 50
        """).fetchone()[0]

        st.write(
            f"""
            **Insight:**  
            Trip distances are mostly on the short side. The median distance is **{median_dist} miles**, and **{short_share}%** of trips are **2 miles or less**, which shows a lot of rides are quick “get me from here to there” trips. You can see that in the shape of the distribution: it’s heavy near the low end and then thins out as distance increases.

            Longer rides exist, but they make up a smaller portion. For example, only about **{long_share}%** of trips are **10 miles or more** (within the 0–50 mile range). So overall, this filtered dataset looks like it’s driven by short, frequent trips rather than long journeys.
            """
        )

    st.divider()

    # u) Pie: Payment type breakdown
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
            lead = top_pct - second_pct
        else:
            second, second_pct, lead = None, None, None

        st.write(
            f"""
            **Insight:**  
            Payment behavior isn’t evenly split — one option usually comes out as the default. Here, **{top_p.payment_name}** leads with **{top_pct:.2f}%** of trips. This tells you what most riders are using in this filtered view, and it sets the tone for how trips are being paid for overall.

            {"" if second is None else f"The next most common option is **{second.payment_name}** at **{second_pct:.2f}%**, so the lead is about **{lead:.2f} percentage points**. That gap matters because it shows whether riders are split between methods or leaning strongly in one direction."}
            """
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

        total_cells = df_v["trip_count"].sum()
        peak_pct = (int(peak_cell.trip_count) / total_cells) * 100 if total_cells else 0

        low_cell = df_v.loc[df_v["trip_count"].idxmin()]
        low_pct = (int(low_cell.trip_count) / total_cells) * 100 if total_cells else 0

        st.write(
            f"""
            **Insight:**  
            The busiest point on the heatmap is **{peak_cell.day_of_week}** around **{int(peak_cell.pickup_hour)}:00**, with **{int(peak_cell.trip_count):,} trips** in that slot. That single time window makes up about **{peak_pct:.2f}%** of all trips counted in this heatmap, so it’s a real spike, not noise.

            What’s interesting is the contrast: the quietest window is **{low_cell.day_of_week}** around **{int(low_cell.pickup_hour)}:00**, sitting at roughly **{int(low_cell.trip_count):,} trips** (**{low_pct:.2f}%**). So demand clearly rises and falls across the week, and the “busy hours” depend on the day — it’s not one flat daily pattern.
            """
        )
