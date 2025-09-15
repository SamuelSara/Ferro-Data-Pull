"""FastAPI application exposing ERCOT RTM data."""
from __future__ import annotations

from datetime import datetime
from typing import List

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from app.locations import CANONICAL_LOCATIONS, normalize_location
from app.storage import load_dataset

app = FastAPI(title="ERCOT RTM API", version="0.1.0")


class MarketRow(BaseModel):
    timestamp: datetime
    zone: str
    price: float
    system_load: float
    sentiment: float
    sentiment_bucket: str

    @classmethod
    def from_series(cls, row: pd.Series) -> "MarketRow":
        timestamp = row["timestamp"]
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()
        return cls(
            timestamp=timestamp,
            zone=str(row["zone"]),
            price=float(row["price"]),
            system_load=float(row["system_load"]),
            sentiment=float(row["sentiment"]),
            sentiment_bucket=str(row["sentiment_bucket"]),
        )


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "ERCOT RTM API. Use /latest or /history endpoints."}


def _filter_zone(df: pd.DataFrame, zone: str) -> pd.DataFrame:
    canonical = normalize_location(zone)
    if canonical is None or canonical not in CANONICAL_LOCATIONS:
        raise HTTPException(status_code=404, detail=f"Unknown zone '{zone}'")

    zone_df = df[df["zone"] == canonical]
    if zone_df.empty:
        raise HTTPException(status_code=404, detail=f"No data for zone '{canonical}'")
    return zone_df.sort_values("timestamp")


@app.get("/latest", response_model=MarketRow)
def latest(zone: str = Query(..., description="Zone or hub name")) -> MarketRow:
    df = load_dataset()
    if df.empty:
        raise HTTPException(status_code=404, detail="Dataset is empty")
    zone_df = _filter_zone(df, zone)
    row = zone_df.iloc[-1]
    return MarketRow.from_series(row)


@app.get("/history", response_model=List[MarketRow])
def history(
    zone: str = Query(..., description="Zone or hub name"),
    hours: int = Query(24, gt=0, le=24 * 14, description="Number of hours of history to return"),
) -> List[MarketRow]:
    df = load_dataset()
    if df.empty:
        raise HTTPException(status_code=404, detail="Dataset is empty")

    zone_df = _filter_zone(df, zone)
    latest_timestamp = zone_df["timestamp"].max()
    cutoff = latest_timestamp - pd.Timedelta(hours=hours - 1)
    recent = zone_df[zone_df["timestamp"] >= cutoff]
    return [MarketRow.from_series(row) for _, row in recent.iterrows()]
