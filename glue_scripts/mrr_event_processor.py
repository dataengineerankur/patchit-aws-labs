"""
AWS Glue PySpark Job: MRR Event Processor

Ingests raw subscription billing events from the transactional data lake,
de-duplicates intra-day mutations, computes per-subscription MRR deltas,
and emits a clean event ledger partitioned by billing_period for downstream
cohort and waterfall analysis.

Input  S3 path : s3://saas-datalake/raw/billing_events/
Output S3 path : s3://saas-datalake/processed/mrr_events/
"""

import sys
import logging
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, DateType, TimestampType, BooleanType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mrr_event_processor")

RAW_EVENTS_PATH   = "s3://saas-datalake/raw/billing_events/"
OUTPUT_PATH       = "s3://saas-datalake/processed/mrr_events/"

RAW_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("subscription_id",  StringType(),    False),
    StructField("account_id",       StringType(),    False),
    StructField("event_type",       StringType(),    True),
    StructField("event_ts",         TimestampType(), True),
    StructField("mrr_before_cents", LongType(),      True),
    StructField("mrr_after_cents",  LongType(),      True),
    StructField("plan_id",          StringType(),    True),
    StructField("billing_interval", StringType(),    True),
    StructField("country_code",     StringType(),    True),
    StructField("created_at",       TimestampType(), True),
    StructField("is_trial",         BooleanType(),   True),
])

VALID_EVENT_TYPES = {
    "new_subscription", "upgrade", "downgrade",
    "reactivation", "cancellation", "pause", "plan_change",
}


def load_raw_events(spark: SparkSession) -> "DataFrame":
    df = spark.read.schema(RAW_SCHEMA).parquet(RAW_EVENTS_PATH)
    logger.info("Loaded %d raw billing events", df.count())
    return df


def filter_valid_events(df: "DataFrame") -> "DataFrame":
    return (
        df.filter(F.col("event_type").isin(list(VALID_EVENT_TYPES)))
          .filter(F.col("subscription_id").isNotNull())
          .filter(F.col("event_ts").isNotNull())
          .filter(~F.col("is_trial").cast("boolean"))
    )


def deduplicate_intraday_events(df: "DataFrame") -> "DataFrame":
    """
    Within a single calendar day a subscription may receive multiple mutations
    (e.g. immediate downgrade followed by a retroactive plan correction).
    Retain only the chronologically first mutation per subscription per day
    to preserve the original intent signal; later same-day corrections are
    captured in the next processing window.
    """
    event_date_col = F.to_date(F.col("event_ts")).alias("event_date")
    df = df.withColumn("event_date", F.to_date(F.col("event_ts")))

    dedup_window = (
        Window.partitionBy("subscription_id", "event_date")
              .orderBy(F.col("created_at").asc())
    )
    df = df.withColumn("_row_rank", F.row_number().over(dedup_window))
    df = df.filter(F.col("_row_rank") == 1).drop("_row_rank")
    return df


def compute_mrr_delta(df: "DataFrame") -> "DataFrame":
    """
    Derive the MRR delta for each event.  For new subscriptions the before-MRR
    is zero.  For cancellations the after-MRR is zero.  Both sentinel values
    are guaranteed by the billing platform but we guard against nulls anyway.
    """
    df = df.withColumn(
        "mrr_before_safe",
        F.coalesce(F.col("mrr_before_cents"), F.lit(0))
    )
    df = df.withColumn(
        "mrr_after_safe",
        F.coalesce(F.col("mrr_after_cents"), F.lit(0))
    )
    df = df.withColumn(
        "mrr_delta_cents",
        F.col("mrr_after_safe") - F.col("mrr_before_safe")
    )
    return df


def assign_billing_period(df: "DataFrame") -> "DataFrame":
    """Tag each event with its ISO billing period (YYYY-MM)."""
    return df.withColumn(
        "billing_period",
        F.date_format(F.col("event_ts"), "yyyy-MM")
    )


def compute_period_net_new_mrr(df: "DataFrame") -> "DataFrame":
    """
    Aggregate per-subscription events within a period to arrive at a single
    net-new-MRR figure per subscription per period.  This collapses intra-period
    upgrade → downgrade sequences into their net effect.
    """
    period_window = (
        Window.partitionBy("subscription_id", "billing_period")
              .orderBy(F.col("event_ts").asc())
              .rowsBetween(Window.unboundedPreceding, 0)
    )

    df = df.withColumn(
        "cumulative_mrr_delta_cents",
        F.sum("mrr_delta_cents").over(period_window)
    )

    latest_event_window = (
        Window.partitionBy("subscription_id", "billing_period")
              .orderBy(F.col("event_ts").desc())
    )
    df = df.withColumn("_latest_rank", F.row_number().over(latest_event_window))
    df = df.filter(F.col("_latest_rank") == 1).drop("_latest_rank")

    df = df.withColumn(
        "net_new_mrr_cents",
        F.col("cumulative_mrr_delta_cents")
    )
    df = df.withColumn(
        "net_new_mrr_dollars",
        F.round(F.col("net_new_mrr_cents") / 100.0, 2)
    )
    df = df.withColumn(
        "closing_mrr_dollars",
        F.round(F.col("mrr_after_safe") / 100.0, 2)
    )
    return df


def classify_movement_type(df: "DataFrame") -> "DataFrame":
    """
    Assign a standardised movement category to each net event.
    Categories: new_business, expansion, contraction, churned, reactivated, neutral
    """
    return df.withColumn(
        "movement_type",
        F.when(F.col("event_type") == "new_subscription",  F.lit("new_business"))
         .when(F.col("event_type") == "reactivation",       F.lit("reactivated"))
         .when(
             (F.col("event_type").isin("upgrade", "plan_change")) &
             (F.col("net_new_mrr_cents") > 0),
             F.lit("expansion")
         )
         .when(
             (F.col("event_type").isin("downgrade", "plan_change")) &
             (F.col("net_new_mrr_cents") < 0),
             F.lit("contraction")
         )
         .when(F.col("event_type") == "cancellation",       F.lit("churned"))
         .otherwise(F.lit("neutral"))
    )


def enrich_with_account_metadata(df: "DataFrame", spark: SparkSession) -> "DataFrame":
    account_meta_path = "s3://saas-datalake/reference/account_metadata/"
    try:
        meta_df = spark.read.parquet(account_meta_path).select(
            "account_id", "account_tier", "csm_owner", "industry_vertical",
            "geo_region", "first_paid_date"
        )
        return df.join(meta_df, on="account_id", how="left")
    except Exception as exc:
        logger.warning("Account metadata unavailable (%s) — skipping enrichment", exc)
        null_cols = ["account_tier", "csm_owner", "industry_vertical", "geo_region", "first_paid_date"]
        for col in null_cols:
            df = df.withColumn(col, F.lit(None).cast(StringType()))
        return df


def write_output(df: "DataFrame") -> None:
    output_cols = [
        "event_id", "subscription_id", "account_id", "billing_period",
        "event_date", "event_type", "movement_type",
        "mrr_before_safe", "mrr_after_safe", "mrr_delta_cents",
        "net_new_mrr_cents", "net_new_mrr_dollars", "closing_mrr_dollars",
        "plan_id", "billing_interval", "country_code",
        "account_tier", "csm_owner", "industry_vertical", "geo_region",
    ]
    (
        df.select(output_cols)
          .repartition(32, F.col("billing_period"))
          .write
          .mode("overwrite")
          .partitionBy("billing_period")
          .parquet(OUTPUT_PATH)
    )
    logger.info("MRR events written to %s", OUTPUT_PATH)


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc   = SparkContext()
    gc   = GlueContext(sc)
    spark = gc.spark_session
    job  = Job(gc)
    job.init(args["JOB_NAME"], args)

    raw       = load_raw_events(spark)
    filtered  = filter_valid_events(raw)
    deduped   = deduplicate_intraday_events(filtered)
    with_delta = compute_mrr_delta(deduped)
    with_period = assign_billing_period(with_delta)
    with_net_mrr = compute_period_net_new_mrr(with_period)
    classified = classify_movement_type(with_net_mrr)
    enriched   = enrich_with_account_metadata(classified, spark)

    write_output(enriched)
    job.commit()
    logger.info("mrr_event_processor complete")


if __name__ == "__main__":
    main()
