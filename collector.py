"""Command-line collector for ERCOT RTM data."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from app.fetch import FetchConfig, fetch_dataset
from app.sentiment import compute_sentiment
from app.storage import (
    DATA_PATH,
    append_and_dedupe,
    load_dataset,
    save_dataset,
)

logger = logging.getLogger(__name__)


def collect_and_update(config: FetchConfig, output_path: Path = DATA_PATH) -> pd.DataFrame:
    """Fetch new data, merge with the existing dataset, and persist it."""

    logger.info("Starting data collection")
    fresh = fetch_dataset(config)
    if fresh.empty:
        logger.warning("No new data was returned by gridstatus")
        return load_dataset(output_path)

    logger.info("Loaded %d rows from gridstatus", len(fresh))

    existing = load_dataset(output_path)
    logger.info("Existing dataset contains %d rows", len(existing))

    combined = append_and_dedupe(existing, fresh)
    logger.info("Combined dataset contains %d rows after dedupe", len(combined))

    enriched = compute_sentiment(combined)
    save_dataset(enriched, output_path)
    logger.info("Persisted dataset to %s", output_path)

    return enriched


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect ERCOT RTM data")
    parser.add_argument(
        "--lookback",
        type=int,
        default=FetchConfig.lookback_hours,
        help="Number of hours in the past to refresh from gridstatus",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DATA_PATH,
        help="Path to the parquet file",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    config = FetchConfig(lookback_hours=args.lookback)
    collect_and_update(config, args.output)
