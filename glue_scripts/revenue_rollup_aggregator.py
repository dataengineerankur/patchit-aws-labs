"""
Helper module: Revenue Rollup Aggregator

Computes rolling revenue aggregations (7-day, 30-day) and Bollinger Band
statistics for anomaly detection baseline.

BUG (DATA-4821): Column 'daily_revenue' was renamed to 'revenue_daily' in the
Q4-2024 data lake migration. All references here must be updated.
"""

from __future__ import annotations

import logging
from typing import Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger("revenue_rollup_aggregator")

# BUG: pre-migration column name
REVENUE_COL = "daily_revenue"        # ← WRONG: should be "revenue_daily"
SEGMENT_COL = "segment_id"
DATE_COL = "date"


def compute_rolling_stats(df: DataFrame) -> DataFrame:
    """Compute 7-day and 30-day rolling revenue statistics per segment.

    Returns the input DataFrame enriched with:
      - revenue_7d_avg, revenue_7d_std
      - revenue_30d_avg, revenue_30d_std
      - revenue_bollinger_upper, revenue_bollinger_lower

    BUG: references REVENUE_COL = "daily_revenue" — fails after schema migration.
    """
    base_window = (
        Window.partitionBy(SEGMENT_COL)
        .orderBy(F.unix_timestamp(F.col(DATE_COL)))
    )

    w7 = base_window.rowsBetween(-6, 0)
    w30 = base_window.rowsBetween(-29, 0)

    # 7-day stats — BUG: column "daily_revenue" doesn't exist
    df = df.withColumn("revenue_7d_avg", F.avg(F.col(REVENUE_COL)).over(w7))        # ← WRONG
    df = df.withColumn("revenue_7d_std", F.stddev(F.col(REVENUE_COL)).over(w7))     # ← WRONG

    # 30-day stats — BUG: column "daily_revenue" doesn't exist
    df = df.withColumn("revenue_30d_avg", F.avg(F.col(REVENUE_COL)).over(w30))      # ← WRONG
    df = df.withColumn("revenue_30d_std", F.stddev(F.col(REVENUE_COL)).over(w30))   # ← WRONG

    # Bollinger Bands (2σ)
    df = df.withColumn(
        "revenue_bollinger_upper",
        F.col("revenue_30d_avg") + 2 * F.col("revenue_30d_std")
    )
    df = df.withColumn(
        "revenue_bollinger_lower",
        F.col("revenue_30d_avg") - 2 * F.col("revenue_30d_std")
    )

    logger.info("Computed rolling statistics (7d and 30d windows)")
    return df


def compute_revenue_zscore(df: DataFrame) -> DataFrame:
    """Add a z-score column measuring deviation from 30-day baseline.

    BUG: references REVENUE_COL = "daily_revenue" — column not found post-migration.
    """
    df = df.withColumn(
        "revenue_zscore",
        F.when(
            F.col("revenue_30d_std") > 0,
            (F.col(REVENUE_COL) - F.col("revenue_30d_avg")) / F.col("revenue_30d_std")  # ← WRONG
        ).otherwise(F.lit(0.0))
    )
    return df


def compute_week_over_week_change(df: DataFrame) -> DataFrame:
    """Compute week-over-week percentage change in daily revenue per segment."""
    lag_window = (
        Window.partitionBy(SEGMENT_COL)
        .orderBy(F.unix_timestamp(F.col(DATE_COL)))
    )
    df = df.withColumn(
        "revenue_lag_7d",
        F.lag(F.col(REVENUE_COL), 7).over(lag_window)          # ← WRONG: "daily_revenue"
    )
    df = df.withColumn(
        "revenue_wow_pct_change",
        F.when(
            F.col("revenue_lag_7d").isNotNull() & (F.col("revenue_lag_7d") != 0),
            F.round(
                (F.col(REVENUE_COL) - F.col("revenue_lag_7d")) / F.col("revenue_lag_7d") * 100,  # ← WRONG
                2
            )
        ).otherwise(F.lit(None))
    )
    return df
