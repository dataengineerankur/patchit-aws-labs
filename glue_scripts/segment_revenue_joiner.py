"""
Helper module: Segment Revenue Joiner

Joins daily revenue records with customer segment metadata and computes
per-segment revenue aggregations.

BUG (DATA-4821): Column 'daily_revenue' was renamed to 'revenue_daily' in the
Q4-2024 data lake migration. All references here must be updated.
"""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

logger = logging.getLogger("segment_revenue_joiner")

# BUG: old column name from pre-migration schema
REVENUE_COL = "daily_revenue"        # ← WRONG: should be "revenue_daily"
SEGMENT_COL = "segment_id"
DATE_COL = "date"


def join_segment_revenue(
    revenue_df: DataFrame,
    segment_meta_df: Optional[DataFrame] = None,
    date_filter: Optional[str] = None,
) -> DataFrame:
    """Join revenue records with segment metadata and compute totals.

    Parameters
    ----------
    revenue_df:
        Daily revenue records with columns: date, segment_id, daily_revenue (WRONG — should be revenue_daily),
        customer_count, avg_order_value.
    segment_meta_df:
        Optional segment metadata (tier, region, etc.) to join on segment_id.
    date_filter:
        Optional ISO date string (YYYY-MM-DD) to filter records.

    Returns
    -------
    DataFrame with per-segment revenue aggregations.
    """
    if date_filter:
        revenue_df = revenue_df.filter(F.col(DATE_COL) >= date_filter)
        logger.info("Applied date filter: %s", date_filter)

    # BUG: REVENUE_COL = "daily_revenue" — this column does not exist after migration
    joined = revenue_df.groupBy(SEGMENT_COL, DATE_COL).agg(
        F.sum(F.col(REVENUE_COL)).alias("segment_revenue_total"),      # ← WRONG
        F.avg(F.col(REVENUE_COL)).alias("segment_revenue_avg"),        # ← WRONG
        F.max(F.col(REVENUE_COL)).alias("segment_revenue_peak"),       # ← WRONG
        F.count("*").alias("transaction_count"),
    )

    if segment_meta_df is not None:
        joined = joined.join(segment_meta_df, on=SEGMENT_COL, how="left")
        logger.info("Joined with segment metadata")

    return joined


def compute_segment_revenue_rank(df: DataFrame) -> DataFrame:
    """Rank segments by total revenue within each date partition."""
    window = Window.partitionBy(DATE_COL).orderBy(
        F.col(REVENUE_COL).desc()     # ← WRONG: "daily_revenue" not found after migration
    )
    return df.withColumn("revenue_rank", F.dense_rank().over(window))


def compute_revenue_contribution_pct(df: DataFrame) -> DataFrame:
    """Compute each segment's share of total daily revenue."""
    daily_total_window = Window.partitionBy(DATE_COL)
    daily_total = F.sum(F.col(REVENUE_COL)).over(daily_total_window)   # ← WRONG

    return df.withColumn(
        "revenue_contribution_pct",
        F.round(F.col(REVENUE_COL) / daily_total * 100, 2)             # ← WRONG
    )
