"""
AWS Glue PySpark Job: Revenue Period Allocator

For multi-year and annual contracts the billing platform emits a single
up-front event.  This job reads the MRR event ledger, identifies non-monthly
billing intervals, and pro-rates contract value uniformly across the
contract duration so that monthly GAAP-aligned revenue figures are accurate.

Monthly subscriptions pass through unchanged.
Quarterly contracts: value / 3
Annual contracts:    value / 12
Multi-year (24m):    value / 24

Input  S3 path : s3://saas-datalake/processed/mrr_events/
Ref    S3 path : s3://saas-datalake/reference/contract_schedule/
Output S3 path : s3://saas-datalake/processed/allocated_revenue/
"""

import sys
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, IntegerType, DateType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("revenue_period_allocator")

MRR_EVENTS_PATH    = "s3://saas-datalake/processed/mrr_events/"
CONTRACT_REF_PATH  = "s3://saas-datalake/reference/contract_schedule/"
OUTPUT_PATH        = "s3://saas-datalake/processed/allocated_revenue/"

CONTRACT_SCHEMA = StructType([
    StructField("subscription_id",    StringType(), False),
    StructField("contract_start",     DateType(),   True),
    StructField("contract_end",       DateType(),   True),
    StructField("contract_arr_cents", LongType(),   True),
    StructField("billing_interval",   StringType(), True),
    StructField("auto_renew",         StringType(), True),
])

BILLING_INTERVAL_MONTHS = {
    "monthly":    1,
    "quarterly":  3,
    "annual":    12,
    "biennial":  24,
}


def load_inputs(spark: SparkSession):
    events_df = spark.read.parquet(MRR_EVENTS_PATH)
    logger.info("Loaded %d MRR events", events_df.count())

    try:
        contract_df = spark.read.schema(CONTRACT_SCHEMA).parquet(CONTRACT_REF_PATH)
    except Exception:
        logger.warning("Contract schedule not found — deriving intervals from event ledger")
        contract_df = None

    return events_df, contract_df


def resolve_contract_duration_months(df: "DataFrame") -> "DataFrame":
    """
    For each subscription determine the number of months the contract spans.
    If a reference contract schedule is unavailable, infer duration from the
    billing_interval field in the event ledger.
    """
    interval_map = F.create_map(
        *[item for pair in [
            (F.lit(k), F.lit(v)) for k, v in BILLING_INTERVAL_MONTHS.items()
        ] for item in pair]
    )

    df = df.withColumn(
        "contract_duration_months",
        F.coalesce(
            interval_map[F.lower(F.col("billing_interval"))],
            F.lit(1)
        )
    )
    return df


def compute_monthly_allocated_revenue(df: "DataFrame") -> "DataFrame":
    """
    Divide each billing event's MRR into equal monthly allocations.

    For monthly subscriptions the allocation is identity (value ÷ 1).
    For quarterly contracts the event MRR represents 3 months of value so
    each allocated month receives one-third.
    """
    df = df.withColumn(
        "allocated_mrr_cents_per_month",
        F.round(
            F.col("closing_mrr_dollars") * 100.0 / F.col("contract_duration_months"),
            0
        ).cast(LongType())
    )
    df = df.withColumn(
        "allocated_mrr_dollars_per_month",
        F.round(F.col("allocated_mrr_cents_per_month") / 100.0, 2)
    )
    return df


def expand_contract_to_monthly_rows(df: "DataFrame", spark: SparkSession) -> "DataFrame":
    """
    For non-monthly contracts, explode the single event row into N monthly rows
    (one per allocated month) so that downstream aggregations can be performed
    at month granularity without special-casing billing intervals.
    """
    df = df.withColumn(
        "month_offsets",
        F.sequence(F.lit(0), F.col("contract_duration_months") - 1)
    )
    df = df.withColumn("month_offset", F.explode(F.col("month_offsets")))
    df = df.withColumn(
        "allocated_period",
        F.date_format(
            F.add_months(
                F.to_date(F.concat(F.col("billing_period"), F.lit("-01"))),
                F.col("month_offset")
            ),
            "yyyy-MM"
        )
    )
    df = df.drop("month_offsets", "month_offset")
    return df


def compute_deferred_revenue_balance(df: "DataFrame") -> "DataFrame":
    """
    For any event row compute how much of the up-front payment has been
    recognised versus how much remains deferred.

    recognised_revenue = allocated_mrr × months_elapsed
    deferred_balance   = total_contract_mrr × duration - recognised_revenue
    """
    df = df.withColumn(
        "months_elapsed",
        F.months_between(
            F.to_date(F.concat(F.col("allocated_period"), F.lit("-01"))),
            F.to_date(F.concat(F.col("billing_period"),   F.lit("-01")))
        ).cast(IntegerType())
    )

    df = df.withColumn(
        "recognised_revenue_dollars",
        F.col("allocated_mrr_dollars_per_month") * (F.col("months_elapsed") + 1)
    )

    df = df.withColumn(
        "deferred_revenue_balance_dollars",
        F.greatest(
            F.lit(0.0),
            (F.col("closing_mrr_dollars") * F.col("contract_duration_months"))
            - F.col("recognised_revenue_dollars")
        )
    )
    return df


def apply_revenue_recognition_cutoff(df: "DataFrame") -> "DataFrame":
    """
    GAAP revenue recognition stops at the contract end date.  Mark any
    allocation that falls beyond the contract end as unrecognisable so the
    finance team can review and either write off or renew.
    """
    current_period = F.date_format(F.current_date(), "yyyy-MM")
    df = df.withColumn(
        "recognition_status",
        F.when(F.col("allocated_period") > current_period, F.lit("future"))
         .when(F.col("allocated_period") == current_period, F.lit("current"))
         .otherwise(F.lit("recognised"))
    )
    return df


def reconcile_allocated_vs_billed(df: "DataFrame") -> "DataFrame":
    """
    Cross-check that sum of allocated MRR per original billing period equals
    the original closing MRR.  Discrepancies indicate rounding issues.
    """
    recon_window = Window.partitionBy("subscription_id", "billing_period")
    df = df.withColumn(
        "sum_allocated_mrr",
        F.sum("allocated_mrr_dollars_per_month").over(recon_window)
    )
    df = df.withColumn(
        "allocation_rounding_diff",
        F.round(F.col("sum_allocated_mrr") - F.col("closing_mrr_dollars"), 4)
    )
    return df


def write_output(df: "DataFrame") -> None:
    output_cols = [
        "subscription_id", "account_id", "billing_period", "allocated_period",
        "contract_duration_months", "movement_type",
        "allocated_mrr_cents_per_month", "allocated_mrr_dollars_per_month",
        "recognised_revenue_dollars", "deferred_revenue_balance_dollars",
        "recognition_status", "allocation_rounding_diff",
        "account_tier", "industry_vertical", "geo_region", "cohort_month",
    ]
    available = [c for c in output_cols if c in df.columns]
    (
        df.select(available)
          .repartition(32, F.col("allocated_period"))
          .write
          .mode("overwrite")
          .partitionBy("allocated_period")
          .parquet(OUTPUT_PATH)
    )
    logger.info("Allocated revenue written to %s", OUTPUT_PATH)


def main():
    args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc    = SparkContext()
    gc    = GlueContext(sc)
    spark = gc.spark_session
    job   = Job(gc)
    job.init(args["JOB_NAME"], args)

    events_df, contract_df = load_inputs(spark)

    df = resolve_contract_duration_months(events_df)
    df = compute_monthly_allocated_revenue(df)
    df = expand_contract_to_monthly_rows(df, spark)
    df = compute_deferred_revenue_balance(df)
    df = apply_revenue_recognition_cutoff(df)
    df = reconcile_allocated_vs_billed(df)

    if contract_df is not None:
        df = df.join(
            contract_df.select("subscription_id", "contract_arr_cents", "auto_renew"),
            on="subscription_id",
            how="left"
        )

    write_output(df)
    job.commit()
    logger.info("revenue_period_allocator complete")


if __name__ == "__main__":
    main()
