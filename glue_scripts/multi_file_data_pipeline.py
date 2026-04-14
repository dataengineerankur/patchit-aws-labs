"""
AWS Glue PySpark Job: Multi-File Data Pipeline

Orchestrates the full revenue analytics pipeline:
  1. Load daily revenue + RFM segment data from S3
  2. Join segments to revenue records
  3. Compute rolling aggregations
  4. Score anomalies against dynamic thresholds
  5. Export segment performance report

Schema (after 2024-Q4 migration):
  Input columns: date, segment_id, revenue_daily, customer_count, avg_order_value

NOTE: This pipeline and its helper modules were written against the OLD schema
where the revenue column was named 'daily_revenue'. After the S3 data lake
migration (ticket DATA-4821) the column was renamed to 'revenue_daily'.
All references below must be updated to match the new schema.
"""

import sys
import logging
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, DateType, LongType
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("multi_file_pipeline")

# ---------------------------------------------------------------------------
# Schema — BUG: column was renamed from 'daily_revenue' to 'revenue_daily'
#              in the Q4-2024 data lake migration (DATA-4821).
#              This file and all imported helpers must use 'revenue_daily'.
# ---------------------------------------------------------------------------
REVENUE_COL = "daily_revenue"          # ← WRONG: should be "revenue_daily"
SEGMENT_COL = "segment_id"
DATE_COL = "date"
CUSTOMER_COUNT_COL = "customer_count"
AVG_ORDER_COL = "avg_order_value"

# S3 input/output paths
S3_INPUT_PATH = "s3://patchit-data-lake/revenue/daily/"
S3_OUTPUT_PATH = "s3://patchit-data-lake/reports/segment_performance/"


def load_revenue_data(glue_context: GlueContext, spark: SparkSession):
    """Load daily revenue records from S3."""
    schema = StructType([
        StructField("date", DateType(), nullable=False),
        StructField("segment_id", StringType(), nullable=False),
        StructField("revenue_daily", DoubleType(), nullable=True),   # actual S3 column name
        StructField("customer_count", LongType(), nullable=True),
        StructField("avg_order_value", DoubleType(), nullable=True),
    ])

    df = spark.read.schema(schema).parquet(S3_INPUT_PATH)
    logger.info("Loaded %d revenue records from S3", df.count())
    return df


def compute_7day_rolling_average(df):
    """Compute 7-day rolling average of revenue per segment."""
    window_7d = (
        Window.partitionBy(SEGMENT_COL)
        .orderBy(F.unix_timestamp(DATE_COL))
        .rowsBetween(-6, 0)
    )
    # BUG: REVENUE_COL = "daily_revenue" — column doesn't exist after migration
    df = df.withColumn(
        "revenue_7d_avg",
        F.avg(F.col(REVENUE_COL)).over(window_7d)          # ← WRONG column reference
    )
    df = df.withColumn(
        "revenue_7d_total",
        F.sum(F.col(REVENUE_COL)).over(window_7d)          # ← WRONG column reference
    )
    return df


def flag_revenue_spikes(df, spike_multiplier: float = 2.5):
    """Flag records where daily revenue exceeds N× the 7-day average."""
    df = df.withColumn(
        "is_spike",
        F.when(
            F.col(REVENUE_COL) > F.col("revenue_7d_avg") * spike_multiplier,   # ← WRONG
            True
        ).otherwise(False)
    )
    df = df.withColumn(
        "spike_ratio",
        F.col(REVENUE_COL) / F.col("revenue_7d_avg")       # ← WRONG
    )
    return df


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("Starting multi-file data pipeline job")

    # Stage 1 — Load
    revenue_df = load_revenue_data(glue_context, spark)

    # Stage 2 — Rolling aggregations (fails here due to wrong column name)
    enriched_df = compute_7day_rolling_average(revenue_df)

    # Stage 3 — Anomaly flagging
    flagged_df = flag_revenue_spikes(enriched_df)

    # Stage 4 — Compute segment totals (calls helper modules conceptually)
    segment_totals = flagged_df.groupBy(SEGMENT_COL).agg(
        F.sum(F.col(REVENUE_COL)).alias("total_revenue"),       # ← WRONG
        F.avg(F.col(REVENUE_COL)).alias("avg_daily_revenue"),   # ← WRONG
        F.count("*").alias("record_count"),
        F.sum(F.col("is_spike").cast("long")).alias("spike_count"),
    )

    # Stage 5 — Output
    segment_totals.write.mode("overwrite").parquet(S3_OUTPUT_PATH)
    logger.info("Pipeline complete — output written to %s", S3_OUTPUT_PATH)

    job.commit()


if __name__ == "__main__":
    main()
