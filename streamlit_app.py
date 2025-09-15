"""Streamlit dashboard for ERCOT RTM data and sentiment."""
from __future__ import annotations

from zoneinfo import ZoneInfo

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from app.sentiment import SENTIMENT_THRESHOLDS
from app.storage import get_available_zones, load_dataset

TIMEZONES = {
    "UTC": ZoneInfo("UTC"),
    "CST/CDT": ZoneInfo("America/Chicago"),
}

COLOR_MAP = {
    "green": "#2ecc71",
    "yellow": "#f1c40f",
    "red": "#e74c3c",
}


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    return load_dataset()


def convert_timezone(df: pd.DataFrame, tz: ZoneInfo) -> pd.DataFrame:
    if df.empty:
        return df
    converted = df.copy()
    converted["timestamp"] = converted["timestamp"].dt.tz_convert(tz)
    return converted


def format_number(value: float, suffix: str) -> str:
    return f"{value:,.2f}{suffix}"


def build_price_load_chart(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["price"],
            mode="lines",
            name="RTM Price ($/MWh)",
            line=dict(color="#1f77b4"),
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=df["timestamp"],
            y=df["system_load"],
            name="System Load (MW)",
            marker_color="#ff7f0e",
            opacity=0.6,
        ),
        secondary_y=True,
    )
    fig.update_layout(
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Price ($/MWh)", secondary_y=False)
    fig.update_yaxes(title_text="System Load (MW)", secondary_y=True)
    return fig


def build_sentiment_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for bucket, color in COLOR_MAP.items():
        subset = df[df["sentiment_bucket"] == bucket]
        if subset.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=subset["timestamp"],
                y=subset["sentiment"],
                mode="lines+markers",
                name=bucket.capitalize(),
                marker=dict(color=color, size=8),
                line=dict(color=color, width=2),
            )
        )
    fig.add_hrect(
        y0=SENTIMENT_THRESHOLDS["green"],
        y1=100,
        fillcolor=COLOR_MAP["green"],
        opacity=0.1,
        line_width=0,
    )
    fig.add_hrect(
        y0=SENTIMENT_THRESHOLDS["yellow"],
        y1=SENTIMENT_THRESHOLDS["green"],
        fillcolor=COLOR_MAP["yellow"],
        opacity=0.1,
        line_width=0,
    )
    fig.add_hrect(
        y0=0,
        y1=SENTIMENT_THRESHOLDS["yellow"],
        fillcolor=COLOR_MAP["red"],
        opacity=0.1,
        line_width=0,
    )
    fig.update_layout(
        margin=dict(l=40, r=40, t=40, b=40),
        yaxis=dict(range=[0, 100], title="Sentiment"),
    )
    return fig


def main() -> None:
    st.set_page_config(page_title="ERCOT RTM Dashboard", layout="wide")
    st.title("ERCOT Real-Time Market Dashboard")

    data = load_data()

    if data.empty:
        st.info("No data is available yet. Run `collector.py` to populate the dataset.")
        return

    zones = get_available_zones(data)
    default_zone = zones[0] if zones else None
    selected_zone = st.selectbox("Select Zone or Hub", options=zones, index=0 if default_zone else None)

    tz_label = st.radio("Timezone", options=list(TIMEZONES.keys()), horizontal=True)
    tz = TIMEZONES[tz_label]

    zone_data = data[data["zone"] == selected_zone]
    zone_data = zone_data.sort_values("timestamp")
    zone_data = convert_timezone(zone_data, tz)

    latest = zone_data.iloc[-1]
    st.markdown("### Key Metrics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Price", format_number(latest["price"], " $/MWh"))
    col2.metric("System Load", format_number(latest["system_load"], " MW"))
    col3.metric("Sentiment", f"{latest['sentiment']:.1f}",
                delta=f"Bucket: {latest['sentiment_bucket'].capitalize()}")

    tab1, tab2 = st.tabs(["Price & Load", "Sentiment"])

    with tab1:
        st.plotly_chart(build_price_load_chart(zone_data), use_container_width=True)

    with tab2:
        st.plotly_chart(build_sentiment_chart(zone_data), use_container_width=True)

    st.caption(
        "Sentiment weighting: 60% price deviation, 40% load stress."
        " Values above 70 are favorable, 40–69 cautionary, below 40 poor."
    )


if __name__ == "__main__":
    main()
