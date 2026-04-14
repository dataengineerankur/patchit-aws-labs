"""
Helper module: Anomaly Threshold Scorer

Scores daily revenue records against static and dynamic thresholds
and emits structured anomaly alert payloads.

BUG (DATA-4821): Column 'daily_revenue' was renamed to 'revenue_daily' in the
Q4-2024 data lake migration. All references here must be updated.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("anomaly_threshold_scorer")

# BUG: pre-migration column name
REVENUE_COL = "daily_revenue"        # ← WRONG: should be "revenue_daily"
SEGMENT_COL = "segment_id"
DATE_COL = "date"

# Alert severity thresholds
ZSCORE_P2_THRESHOLD = 2.0   # moderate anomaly
ZSCORE_P1_THRESHOLD = 3.5   # severe anomaly
SPIKE_RATIO_THRESHOLD = 2.5  # 2.5× the 7-day average


@dataclass
class AnomalyAlert:
    segment_id: str
    date: str
    revenue_actual: float
    revenue_expected: float
    zscore: float
    severity: str
    description: str


def score_anomalies(
    df: DataFrame,
    zscore_threshold: float = ZSCORE_P2_THRESHOLD,
    spike_multiplier: float = SPIKE_RATIO_THRESHOLD,
) -> DataFrame:
    """Flag rows that exceed the anomaly thresholds.

    Adds columns: anomaly_severity, anomaly_description.

    BUG: references REVENUE_COL = "daily_revenue" — column not found after migration.
    """
    # Spike ratio: current vs 7-day average — BUG: "daily_revenue" not found
    df = df.withColumn(
        "spike_ratio",
        F.when(
            F.col("revenue_7d_avg").isNotNull() & (F.col("revenue_7d_avg") > 0),
            F.col(REVENUE_COL) / F.col("revenue_7d_avg")            # ← WRONG
        ).otherwise(F.lit(1.0))
    )

    # Anomaly severity classification
    df = df.withColumn(
        "anomaly_severity",
        F.when(
            F.col("revenue_zscore").isNotNull() &
            (F.abs(F.col("revenue_zscore")) >= ZSCORE_P1_THRESHOLD),
            F.lit("P1_CRITICAL")
        ).when(
            F.col("revenue_zscore").isNotNull() &
            (F.abs(F.col("revenue_zscore")) >= ZSCORE_P2_THRESHOLD),
            F.lit("P2_WARNING")
        ).when(
            F.col("spike_ratio") >= spike_multiplier,
            F.lit("P2_WARNING")
        ).otherwise(F.lit("NORMAL"))
    )

    # Anomaly description — BUG: "daily_revenue" not found
    df = df.withColumn(
        "anomaly_description",
        F.when(
            F.col("anomaly_severity") != "NORMAL",
            F.concat(
                F.lit("Segment "), F.col(SEGMENT_COL),
                F.lit(" recorded revenue="),
                F.col(REVENUE_COL).cast("string"),                  # ← WRONG
                F.lit(" vs 7d_avg="),
                F.round(F.col("revenue_7d_avg"), 2).cast("string"),
                F.lit(" (z="),
                F.round(F.col("revenue_zscore"), 2).cast("string"),
                F.lit(")")
            )
        ).otherwise(F.lit(None))
    )

    anomaly_count = df.filter(F.col("anomaly_severity") != "NORMAL").count()
    logger.info("Scored anomalies — %d records flagged", anomaly_count)
    return df


def filter_anomalies_only(df: DataFrame) -> DataFrame:
    """Return only the anomalous rows for downstream alerting."""
    return df.filter(F.col("anomaly_severity") != "NORMAL").select(
        DATE_COL,
        SEGMENT_COL,
        REVENUE_COL,            # ← WRONG: "daily_revenue" not found after migration
        "revenue_7d_avg",
        "revenue_30d_avg",
        "revenue_zscore",
        "spike_ratio",
        "anomaly_severity",
        "anomaly_description",
    )


def compute_anomaly_rate_by_segment(df: DataFrame) -> DataFrame:
    """Compute what fraction of days were anomalous per segment."""
    return df.groupBy(SEGMENT_COL).agg(
        F.count("*").alias("total_days"),
        F.sum(
            F.when(F.col("anomaly_severity") != "NORMAL", 1).otherwise(0)
        ).alias("anomaly_days"),
        F.avg(F.col(REVENUE_COL)).alias("avg_revenue"),             # ← WRONG
        F.stddev(F.col(REVENUE_COL)).alias("stddev_revenue"),       # ← WRONG
    ).withColumn(
        "anomaly_rate_pct",
        F.round(F.col("anomaly_days") / F.col("total_days") * 100, 2)
    )
