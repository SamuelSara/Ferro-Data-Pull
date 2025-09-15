"""Utilities for fetching ERCOT real-time market data."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

import pandas as pd

try:
    from gridstatus import Ercot  # type: ignore
except ImportError as exc:  # pragma: no cover - handled at runtime
    raise ImportError(
        "The gridstatus package is required to fetch ERCOT data."
    ) from exc

from app.locations import CANONICAL_LOCATIONS, normalize_location

logger = logging.getLogger(__name__)

# Columns that may represent timestamps in gridstatus DataFrames
_TIME_COLUMN_CANDIDATES = [
    "timestamp",
    "time",
    "datetime",
    "Delivery Interval",
    "Delivery Interval Ending",
    "Interval Ending",
    "Oper Interval",
    "Settlement Point Price Date",
    "Settlement Point Price Timestamp",
]

# Columns that may contain location/settlement point names
_LOCATION_COLUMN_CANDIDATES = [
    "location",
    "Location",
    "Settlement Point",
    "settlement_point",
    "Load Zone",
    "SettlementPoint",
]

# Columns that may contain RTM price values
_PRICE_COLUMN_CANDIDATES = [
    "lmp",
    "price",
    "Price",
    "Settlement Point Price",
    "Settlement Point Price ($/MWH)",
    "SettlementPointPrice",
]

# Columns that may contain system load values
_LOAD_COLUMN_CANDIDATES = [
    "System Load",
    "Actual System Load",
    "Actual Load",
    "Load",
    "Actual Load (MW)",
    "actual_load",
]


@dataclass
class FetchConfig:
    """Configuration for data collection."""

    lookback_hours: int = 48

    @property
    def start_time(self) -> datetime:
        """Start timestamp for data collection in UTC."""
        end = datetime.now(tz=timezone.utc)
        return end - timedelta(hours=self.lookback_hours)

    @property
    def end_time(self) -> datetime:
        """End timestamp for data collection in UTC."""
        return datetime.now(tz=timezone.utc)


def _detect_column(frame: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    for col in candidates:
        if col in frame.columns:
            return col
    return None


def _prepare_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize various gridstatus price frames into a standard layout."""

    frame = frame.copy()

    ts_col = _detect_column(frame, _TIME_COLUMN_CANDIDATES)
    loc_col = _detect_column(frame, _LOCATION_COLUMN_CANDIDATES)
    price_col = _detect_column(frame, _PRICE_COLUMN_CANDIDATES)

    if not ts_col or not loc_col or not price_col:
        missing = {
            "timestamp": ts_col,
            "location": loc_col,
            "price": price_col,
        }
        raise ValueError(f"Unable to normalize price frame – missing columns: {missing}")

    frame["timestamp"] = pd.to_datetime(frame[ts_col], utc=True, errors="coerce")
    frame["zone"] = frame[loc_col].map(normalize_location)
    frame["price"] = pd.to_numeric(frame[price_col], errors="coerce")

    frame = frame.dropna(subset=["timestamp", "zone", "price"])
    frame["timestamp"] = frame["timestamp"].dt.floor("H")

    # Average within the hour in case higher frequency data is returned
    agg = (
        frame.groupby(["zone", "timestamp"], as_index=False)["price"].mean()
    )

    return agg


def _prepare_load_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize system load frames into a standard layout."""

    frame = frame.copy()

    ts_col = _detect_column(frame, _TIME_COLUMN_CANDIDATES)
    load_col = _detect_column(frame, _LOAD_COLUMN_CANDIDATES)

    if not ts_col or not load_col:
        missing = {"timestamp": ts_col, "load": load_col}
        raise ValueError(f"Unable to normalize load frame – missing columns: {missing}")

    frame["timestamp"] = pd.to_datetime(frame[ts_col], utc=True, errors="coerce")
    frame["system_load"] = pd.to_numeric(frame[load_col], errors="coerce")

    frame = frame.dropna(subset=["timestamp", "system_load"])
    frame["timestamp"] = frame["timestamp"].dt.floor("H")

    agg = (
        frame.groupby("timestamp", as_index=False)["system_load"].mean()
    )

    return agg


def _call_ercot(method_names: Iterable[str], *args, **kwargs) -> pd.DataFrame:
    """Call one of the potential gridstatus methods that provide data."""

    client = Ercot()
    last_error: Optional[Exception] = None

    for name in method_names:
        if not hasattr(client, name):
            continue
        method = getattr(client, name)
        try:
            logger.debug("Calling gridstatus.Ercot.%s", name)
            return method(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - depends on gridstatus runtime
            logger.warning("gridstatus method %s failed: %s", name, exc)
            last_error = exc
    if last_error:
        raise last_error
    raise AttributeError(
        f"gridstatus.Ercot is missing expected methods: {', '.join(method_names)}"
    )


def fetch_rtm_prices(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch RTM prices from ERCOT for the requested interval."""

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    method_names = ["get_lmp", "get_prices", "get_real_time_prices"]

    frames: List[pd.DataFrame] = []
    try:
        raw = _call_ercot(method_names, start=start, end=end, market="RTM")
        frames.append(_prepare_price_frame(pd.DataFrame(raw)))
    except TypeError:
        # Fallback to per-day requests for gridstatus versions that require a date argument.
        current = start
        while current.date() <= end.date():
            raw = _call_ercot(method_names, date=current.date(), market="RTM")
            frames.append(_prepare_price_frame(pd.DataFrame(raw)))
            current += timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["zone", "timestamp", "price"])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[combined["zone"].isin(CANONICAL_LOCATIONS)]
    combined = combined.drop_duplicates(subset=["zone", "timestamp"])
    return combined.sort_values(["zone", "timestamp"]).reset_index(drop=True)


def fetch_system_load(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch system load for the requested interval."""

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    method_names = ["get_load", "get_system_load", "get_actual_load"]

    frames: List[pd.DataFrame] = []
    try:
        raw = _call_ercot(method_names, start=start, end=end)
        frames.append(_prepare_load_frame(pd.DataFrame(raw)))
    except TypeError:
        current = start
        while current.date() <= end.date():
            raw = _call_ercot(method_names, date=current.date())
            frames.append(_prepare_load_frame(pd.DataFrame(raw)))
            current += timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["timestamp", "system_load"])

    load_frame = pd.concat(frames, ignore_index=True)
    load_frame = load_frame.drop_duplicates(subset=["timestamp"])
    return load_frame.sort_values("timestamp").reset_index(drop=True)


def fetch_dataset(config: Optional[FetchConfig] = None) -> pd.DataFrame:
    """Fetch combined RTM price and system load data for analysis."""

    if config is None:
        config = FetchConfig()

    start = config.start_time
    end = config.end_time

    logger.info("Fetching RTM prices between %s and %s", start, end)
    price_df = fetch_rtm_prices(start, end)

    logger.info("Fetching system load between %s and %s", start, end)
    load_df = fetch_system_load(start, end)

    if price_df.empty:
        return pd.DataFrame(columns=["timestamp", "zone", "price", "system_load"])

    dataset = price_df.merge(load_df, on="timestamp", how="left")
    dataset = dataset.sort_values(["zone", "timestamp"]).reset_index(drop=True)
    dataset["system_load"] = dataset["system_load"].ffill()

    return dataset
