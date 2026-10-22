import json
import math
import uuid
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer

st.set_page_config(page_title="Medicines Dashboard", page_icon="💊", layout="wide")
st.title("💊 Medicine orders: real-time dashboard")
st.caption(
    "Orders come from Kafka (topic: medicines). "
    "Spark statistics come from Spark Structured Streaming (topic: medicines-stats)."
)

TIME_WINDOWS = {
    "All time": None,
    "Last 1 minute": 1,
    "Last 5 minutes": 5,
    "Last 15 minutes": 15,
}


# ---------- Kafka reading ----------

def read_new_messages():
    """Reads new messages from Kafka and stores them in the session."""
    messages = st.session_state.consumer.consume(num_messages=1000, timeout=1.0)

    for msg in messages:
        if msg.error():
            continue
        try:
            data = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue  # ignore non-JSON messages (e.g. typed in the console producer)
        if not isinstance(data, dict):
            continue

        if msg.topic() == "medicines-stats" and "delivery_country" in data:
            st.session_state.spark_stats[data["delivery_country"]] = data

        elif msg.topic() == "medicines" and "name" in data:
            try:
                price = float(data.get("price", 0))
            except (TypeError, ValueError):
                continue
            st.session_state.orders.append({
                "Time": datetime.fromtimestamp(msg.timestamp()[1] / 1000),
                "Medicine": data.get("name"),
                "Category": data.get("category", "Unknown"),
                "Price (€)": price,
                "Country": data.get("delivery_country", "Unknown"),
                "Partition": msg.partition(),
                "Offset": msg.offset(),
            })

    return len(messages)


def init_state():
    """Creates the Kafka consumer once per browser session and loads the history."""
    if "consumer" in st.session_state:
        return

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"dashboard-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(["medicines", "medicines-stats"])

    st.session_state.consumer = consumer
    st.session_state.orders = []
    st.session_state.spark_stats = {}

    with st.spinner("Loading existing messages from Kafka..."):
        empty_rounds = 0
        while empty_rounds < 3:
            if read_new_messages() == 0:
                empty_rounds += 1
            else:
                empty_rounds = 0


def reset_state():
    st.session_state.consumer.close()
    for key in ["consumer", "orders", "spark_stats"]:
        del st.session_state[key]


# ---------- Filters ----------

def options(df, column):
    return sorted(df[column].dropna().unique().tolist()) if not df.empty else []


def build_sidebar(all_orders):
    with st.sidebar:
        st.header("🔎 Filters")
        st.caption("Leave a selection empty to include all values.")

        countries = st.multiselect("Country", options(all_orders, "Country"))
        categories = st.multiselect("Category", options(all_orders, "Category"))
        medicines = st.multiselect("Medicine", options(all_orders, "Medicine"))
        search = st.text_input("Search medicine name", placeholder="e.g. vita")

        if all_orders.empty:
            low, high = 0.0, 20.0
        else:
            low = float(math.floor(all_orders["Price (€)"].min()))
            high = float(math.ceil(all_orders["Price (€)"].max()))
            if low == high:
                high = low + 1
        price_range = st.slider("Price (€)", low, high, (low, high), step=0.5)

        window = st.selectbox("Time window", list(TIME_WINDOWS.keys()))
        partitions = st.multiselect("Kafka partition", options(all_orders, "Partition"))
        nb_latest = st.slider("Latest orders to display", 5, 50, 15, step=5)

        st.divider()
        if st.button("🔄 Refresh filter options"):
            st.rerun()
        if st.button("🗑️ Reload everything from Kafka"):
            reset_state()
            st.rerun()

    return {
        "countries": countries,
        "categories": categories,
        "medicines": medicines,
        "search": search.strip(),
        "price_range": price_range,
        "minutes": TIME_WINDOWS[window],
        "partitions": partitions,
        "nb_latest": nb_latest,
    }


def apply_filters(df, f):
    if df.empty:
        return df
    if f["countries"]:
        df = df[df["Country"].isin(f["countries"])]
    if f["categories"]:
        df = df[df["Category"].isin(f["categories"])]
    if f["medicines"]:
        df = df[df["Medicine"].isin(f["medicines"])]
    if f["search"]:
        df = df[df["Medicine"].str.contains(f["search"], case=False, na=False)]
    low, high = f["price_range"]
    df = df[df["Price (€)"].between(low, high)]
    if f["minutes"]:
        df = df[df["Time"] >= datetime.now() - timedelta(minutes=f["minutes"])]
    if f["partitions"]:
        df = df[df["Partition"].isin(f["partitions"])]
    return df


# ---------- Live view (refreshes every 2 seconds) ----------

@st.fragment(run_every="2s")
def live_view(filters):
    read_new_messages()

    all_orders = pd.DataFrame(st.session_state.orders)
    df = apply_filters(all_orders, filters)

    st.caption(
        f"Showing {len(df)} of {len(all_orders)} orders · "
        f"last refresh {datetime.now():%H:%M:%S}"
    )

    # Key figures
    col1, col2, col3, col4 = st.columns(4)
    revenue = float(df["Price (€)"].sum()) if not df.empty else 0.0
    col1.metric("Orders", len(df))
    col2.metric("Revenue", f"{revenue:.2f} €")
    col3.metric("Average price", f"{revenue / len(df):.2f} €" if len(df) else "0.00 €")
    col4.metric("Countries", df["Country"].nunique() if not df.empty else 0)

    if df.empty:
        st.info("No orders match the current filters (or no orders received yet).")
    else:
        left, right = st.columns(2)
        with left:
            st.subheader("Orders by country")
            st.bar_chart(df.groupby("Country").size().sort_values(ascending=False))
        with right:
            st.subheader("Revenue by category (€)")
            st.bar_chart(df.groupby("Category")["Price (€)"].sum().round(2))

        st.subheader("Orders over time (per 10 seconds)")
        st.line_chart(df.set_index("Time").resample("10s").size())

        left, right = st.columns(2)
        with left:
            st.subheader("Top medicines")
            top = (
                df.groupby("Medicine")
                .agg(Orders=("Medicine", "size"), Revenue=("Price (€)", "sum"))
                .round(2)
                .sort_values("Orders", ascending=False)
            )
            st.dataframe(top)
        with right:
            st.subheader("Orders by partition")
            st.bar_chart(df.groupby("Partition").size())

        st.subheader(f"Latest {filters['nb_latest']} orders")
        latest = df.sort_values("Time", ascending=False).head(filters["nb_latest"]).copy()
        latest["Time"] = latest["Time"].dt.strftime("%H:%M:%S")
        st.dataframe(latest, hide_index=True)

    # Spark results (pre-aggregated, so only the country filter applies)
    st.divider()
    st.subheader("⚡ Statistics computed by Spark")
    st.caption(
        "Spark sends totals per country, already aggregated. "
        "Only the country filter can be applied here."
    )
    spark_df = pd.DataFrame(list(st.session_state.spark_stats.values()))
    if spark_df.empty:
        st.info("Waiting for Spark statistics... Is the Spark job running?")
    else:
        if filters["countries"]:
            spark_df = spark_df[spark_df["delivery_country"].isin(filters["countries"])]
        st.dataframe(
            spark_df.rename(columns={
                "delivery_country": "Country",
                "nb_orders": "Orders",
                "total_eur": "Revenue (€)",
                "avg_price_eur": "Average price (€)",
            }).sort_values("Orders", ascending=False),
            hide_index=True,
        )


# ---------- Main ----------

init_state()
all_orders = pd.DataFrame(st.session_state.orders)
filters = build_sidebar(all_orders)
live_view(filters)