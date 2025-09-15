"""Storage helpers for the ERCOT dataset."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

DATA_PATH = Path("data/ercot.parquet")


def ensure_directory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_dataset(path: Path = DATA_PATH) -> pd.DataFrame:
    """Load the persisted dataset from disk."""

    if not path.exists():
        return pd.DataFrame(columns=["timestamp", "zone", "price", "system_load", "sentiment"])

    df = pd.read_parquet(path)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    return df.sort_values(["zone", "timestamp"]).reset_index(drop=True)


def append_and_dedupe(existing: pd.DataFrame, new_rows: pd.DataFrame) -> pd.DataFrame:
    """Combine new records with existing ones while removing duplicates."""

    if existing.empty:
        combined = new_rows.copy()
    else:
        combined = pd.concat([existing, new_rows], ignore_index=True)

    combined = combined.drop_duplicates(subset=["timestamp", "zone"], keep="last")
    combined = combined.sort_values(["zone", "timestamp"]).reset_index(drop=True)
    return combined


def save_dataset(df: pd.DataFrame, path: Path = DATA_PATH) -> None:
    ensure_directory(path)
    df.to_parquet(path, index=False)


def upsert_dataset(new_rows: pd.DataFrame, path: Path = DATA_PATH) -> pd.DataFrame:
    """Load, merge, de-duplicate, and persist the dataset."""

    existing = load_dataset(path)
    combined = append_and_dedupe(existing, new_rows)
    save_dataset(combined, path)
    return combined


def get_available_zones(df: pd.DataFrame | None = None) -> Iterable[str]:
    if df is None:
        df = load_dataset()
    if df.empty or "zone" not in df.columns:
        return []
    return sorted(df["zone"].unique())
