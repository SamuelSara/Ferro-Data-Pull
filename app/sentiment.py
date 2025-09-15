"""Sentiment scoring utilities."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

SENTIMENT_THRESHOLDS = {
    "green": 70,
    "yellow": 40,
}


@dataclass
class SentimentWeights:
    price: float = 0.6
    load: float = 0.4

    def normalize(self) -> "SentimentWeights":
        total = self.price + self.load
        if total == 0:
            return SentimentWeights(price=0.6, load=0.4)
        return SentimentWeights(price=self.price / total, load=self.load / total)


def _rolling_median(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    return series.rolling(window=window, min_periods=min_periods).median()


def _rolling_mad(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    def mad(values: np.ndarray) -> float:
        median = np.median(values)
        return float(np.median(np.abs(values - median)))

    return series.rolling(window=window, min_periods=min_periods).apply(mad, raw=True)


def _rolling_mean(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    return series.rolling(window=window, min_periods=min_periods).mean()


def _robust_z_score(values: pd.Series, baseline: pd.Series, mad: pd.Series) -> pd.Series:
    mad_safe = mad.copy()
    mad_safe[mad_safe == 0] = np.nan
    z = 0.6745 * (values - baseline) / mad_safe
    return z.fillna(0.0)


def _convert_z_to_score(z: pd.Series) -> pd.Series:
    clipped = z.clip(lower=-2, upper=2)
    score = (1 - ((clipped + 2) / 4.0)) * 100
    return score.clip(lower=0, upper=100)


def _convert_pct_to_score(pct: pd.Series) -> pd.Series:
    clipped = pct.clip(lower=-0.2, upper=0.2)
    score = (1 - ((clipped + 0.2) / 0.4)) * 100
    return score.clip(lower=0, upper=100)


def _sentiment_bucket(value: float) -> Literal["green", "yellow", "red"]:
    if value >= SENTIMENT_THRESHOLDS["green"]:
        return "green"
    if value >= SENTIMENT_THRESHOLDS["yellow"]:
        return "yellow"
    return "red"


def compute_sentiment(df: pd.DataFrame, window_hours: int = 24 * 7,
                      min_periods: int = 24, weights: SentimentWeights | None = None) -> pd.DataFrame:
    """Compute consumer sentiment scores for each zone/hour."""

    if df.empty:
        return df.assign(
            price_baseline=pd.Series(dtype="float64"),
            price_score=pd.Series(dtype="float64"),
            load_baseline=pd.Series(dtype="float64"),
            load_score=pd.Series(dtype="float64"),
            sentiment=pd.Series(dtype="float64"),
            sentiment_bucket=pd.Series(dtype="object"),
        )

    if weights is None:
        weights = SentimentWeights().normalize()
    else:
        weights = weights.normalize()

    df = df.copy()
    df = df.sort_values(["zone", "timestamp"]).reset_index(drop=True)

    grouped = df.groupby("zone", group_keys=False)

    df["price_baseline"] = grouped["price"].transform(
        lambda series: _rolling_median(series, window_hours, min_periods)
    )
    df["price_mad"] = grouped["price"].transform(
        lambda series: _rolling_mad(series, window_hours, min_periods)
    )
    df["price_z"] = _robust_z_score(df["price"], df["price_baseline"], df["price_mad"])
    df["price_score"] = _convert_z_to_score(df["price_z"])

    df["load_baseline"] = grouped["system_load"].transform(
        lambda series: _rolling_mean(series, window_hours, min_periods)
    )
    # Avoid division by zero when baseline is missing.
    df["load_baseline"] = df["load_baseline"].replace(0, np.nan)
    df["load_pct_dev"] = (df["system_load"] - df["load_baseline"]) / df["load_baseline"]
    df["load_pct_dev"] = df["load_pct_dev"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["load_score"] = _convert_pct_to_score(df["load_pct_dev"])

    df["sentiment"] = (
        df["price_score"] * weights.price + df["load_score"] * weights.load
    )
    df["sentiment"] = df["sentiment"].clip(lower=0, upper=100)
    df["sentiment_bucket"] = df["sentiment"].apply(_sentiment_bucket)

    return df
