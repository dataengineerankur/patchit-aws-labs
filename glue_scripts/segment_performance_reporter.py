"""
Helper module: Segment Performance Reporter

Generates the final segment performance report and writes it to S3 in both
Parquet (for downstream analytics) and JSON (for the alerting webhook).

BUG (DATA-4821): Column 'daily_revenue' was renamed to 'revenue_daily' in the
Q4-2024 data lake migration. All references here must be updated.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("segment_performance_reporter")

# BUG: pre-migration column name
REVENUE_COL = "daily_revenue"        # ← WRONG: should be "revenue_daily"
SEGMENT_COL = "segment_id"
DATE_COL = "date"

# Output schema expected by downstream BI dashboards (Looker, Tableau)
REPORT_OUTPUT_COLUMNS = [
    "report_date",
    SEGMENT_COL,
    REVENUE_COL,            # ← WRONG: downstream expects "revenue_daily" after migration
    "revenue_7d_avg",
    "revenue_30d_avg",
    "revenue_zscore",
    "spike_ratio",
    "anomaly_severity",
    "customer_count",
    "avg_order_value",
]

S3_REPORT_PATH = "s3://patchit-data-lake/reports/segment_performance/"
ALERT_WEBHOOK_URL = "https://hooks.example.com/patchit/revenue-anomaly"


def generate_report(
    df: DataFrame,
    report_date: Optional[str] = None,
) -> DataFrame:
    """Select and shape the final report output.

    BUG: REPORT_OUTPUT_COLUMNS includes "daily_revenue" which no longer
    exists after the Q4-2024 schema migration (DATA-4821).
    """
    if report_date:
        df = df.filter(F.col(DATE_COL) == report_date)

    df = df.withColumn("report_date", F.current_date())

    # BUG: REPORT_OUTPUT_COLUMNS references "daily_revenue" — column not found
    available_cols = [c for c in REPORT_OUTPUT_COLUMNS if c in df.columns]
    report_df = df.select(*available_cols)

    logger.info("Generated report with %d rows", report_df.count())
    return report_df


def compute_revenue_summary_stats(df: DataFrame) -> Dict[str, Any]:
    """Compute summary statistics across the whole report period.

    BUG: uses REVENUE_COL = "daily_revenue" — column not found after migration.
    """
    stats_row = df.agg(
        F.sum(F.col(REVENUE_COL)).alias("total_revenue"),            # ← WRONG
        F.avg(F.col(REVENUE_COL)).alias("avg_daily_revenue"),        # ← WRONG
        F.max(F.col(REVENUE_COL)).alias("peak_daily_revenue"),       # ← WRONG
        F.min(F.col(REVENUE_COL)).alias("min_daily_revenue"),        # ← WRONG
        F.countDistinct(SEGMENT_COL).alias("active_segments"),
        F.sum(
            F.when(F.col("anomaly_severity") != "NORMAL", 1).otherwise(0)
        ).alias("total_anomalies"),
    ).first()

    if stats_row is None:
        return {}

    return {
        "total_revenue": stats_row["total_revenue"],
        "avg_daily_revenue": stats_row["avg_daily_revenue"],
        "peak_daily_revenue": stats_row["peak_daily_revenue"],
        "min_daily_revenue": stats_row["min_daily_revenue"],
        "active_segments": stats_row["active_segments"],
        "total_anomalies": stats_row["total_anomalies"],
        "generated_at": datetime.utcnow().isoformat(),
    }


def write_report_to_s3(df: DataFrame, output_path: str = S3_REPORT_PATH) -> None:
    """Write final report DataFrame to S3 as Parquet."""
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Report written to %s", output_path)


def build_alert_payload(
    segment_id: str,
    report_date: str,
    stats: Dict[str, Any],
    anomaly_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Build the JSON payload for the alerting webhook.

    BUG: payload keys reference 'daily_revenue' — consumers expect 'revenue_daily'.
    """
    return {
        "pipeline": "multi_file_data_pipeline",
        "report_date": report_date,
        "segment_id": segment_id,
        "metrics": {
            "daily_revenue": stats.get("avg_daily_revenue"),     # ← WRONG key: should be "revenue_daily"
            "total_revenue": stats.get("total_revenue"),
            "anomaly_count": len(anomaly_rows),
        },
        "anomalies": anomaly_rows,
        "schema_version": "v1",      # should be "v2" after migration
    }
