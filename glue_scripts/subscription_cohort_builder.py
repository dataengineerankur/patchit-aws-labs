"""
AWS Glue PySpark Job: Subscription Cohort Builder

Reads the cleaned MRR event ledger produced by mrr_event_processor and
constructs cohort tables that track per-account ARR evolution from their
first paid date through every subsequent billing period.

The cohort dimension enables NRR, GRR, and logo-churn analysis cut by
acquisition channel, industry vertical, account tier, and geo region.

Input  S3 path : s3://saas-datalake/processed/mrr_events/
Output S3 path : s3://saas-datalake/processed/cohort_arr_matrix/
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
    StringType, LongType, DoubleType, IntegerType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("subscription_cohort_builder")

MRR_EVENTS_PATH   = "s3://saas-datalake/processed/mrr_events/"
OUTPUT_PATH       = "s3://saas-datalake/processed/cohort_arr_matrix/"

COHORT_DIMENSIONS = ["industry_vertical", "geo_region", "account_tier"]


def load_mrr_events(spark: SparkSession) -> "DataFrame":
    df = spark.read.parquet(MRR_EVENTS_PATH)
    logger.info("Loaded MRR events for cohort analysis: %d rows", df.count())
    return df


def derive_cohort_key(df: "DataFrame") -> "DataFrame":
    """
    The cohort key is the ISO year-month of an account's first paid event.
    Accounts with an explicit first_paid_date use that; others fall back to
    the earliest billing_period observed in the ledger.
    """
    first_period_window = (
        Window.partitionBy("account_id")
              .orderBy(F.col("billing_period").asc())
    )

    df = df.withColumn(
        "cohort_month",
        F.when(
            F.col("first_paid_date").isNotNull(),
            F.date_format(
                F.to_date(F.col("first_paid_date"), "yyyy-MM-dd"),
                "yyyy-MM"
            )
        ).otherwise(
            F.first(F.col("billing_period")).over(first_period_window)
        )
    )
    return df


def compute_cohort_index(df: "DataFrame") -> "DataFrame":
    """
    Compute how many complete months have elapsed between the cohort month and
    the observation month.  Index 0 = acquisition month, 1 = first full month
    post-acquisition, etc.
    """
    df = df.withColumn(
        "cohort_index",
        F.months_between(
            F.to_date(F.concat(F.col("billing_period"), F.lit("-01"))),
            F.to_date(F.concat(F.col("cohort_month"),   F.lit("-01")))
        ).cast(IntegerType())
    )
    return df.filter(F.col("cohort_index") >= 0)


def build_account_arr_snapshots(df: "DataFrame") -> "DataFrame":
    """
    For each account × period, produce a closing ARR snapshot by converting
    closing MRR to ARR (× 12) and accumulating the running net change.
    """
    arr_window = (
        Window.partitionBy("account_id")
              .orderBy(F.col("billing_period").asc())
              .rowsBetween(Window.unboundedPreceding, 0)
    )

    df = df.withColumn(
        "cumulative_net_new_mrr_cents",
        F.sum("net_new_mrr_cents").over(arr_window)
    )

    df = df.withColumn(
        "closing_arr_cents",
        F.col("cumulative_net_new_mrr_cents") * 12
    )
    df = df.withColumn(
        "closing_arr_dollars",
        F.round(F.col("closing_arr_cents") / 100.0, 2)
    )
    return df


def compute_beginning_arr(df: "DataFrame") -> "DataFrame":
    """
    Derive the beginning-of-period ARR for each account by lagging the
    closing ARR from the prior period by one billing cycle.
    """
    lag_window = (
        Window.partitionBy("account_id")
              .orderBy(F.col("billing_period").asc())
    )
    df = df.withColumn(
        "beginning_arr_dollars",
        F.lag(F.col("closing_arr_dollars"), 1, 0.0).over(lag_window)
    )
    return df


def tag_expansion_contraction_churn(df: "DataFrame") -> "DataFrame":
    """
    Label each period-level record with its ARR movement category relative
    to beginning ARR so that NRR waterfalls can be constructed downstream.
    """
    delta = F.col("closing_arr_dollars") - F.col("beginning_arr_dollars")

    df = df.withColumn("arr_delta_dollars", delta)
    df = df.withColumn(
        "arr_movement_label",
        F.when(F.col("beginning_arr_dollars") == 0,  F.lit("new_arr"))
         .when(F.col("closing_arr_dollars")   == 0,  F.lit("churned_arr"))
         .when(delta > 0,                            F.lit("expansion_arr"))
         .when(delta < 0,                            F.lit("contraction_arr"))
         .otherwise(F.lit("retained_arr"))
    )
    return df


def aggregate_cohort_matrix(df: "DataFrame") -> "DataFrame":
    """
    Roll up account-level ARR snapshots into a cohort × period matrix.
    Each row in the output represents one cohort observed in one billing
    period, with total ARR and movement components broken out.
    """
    group_keys = ["cohort_month", "cohort_index", "billing_period"] + COHORT_DIMENSIONS

    agg_df = df.groupBy(group_keys).agg(
        F.count("account_id").alias("account_count"),
        F.sum(
            F.when(F.col("arr_movement_label") == "new_arr",         F.col("arr_delta_dollars")).otherwise(0)
        ).alias("new_arr_dollars"),
        F.sum(
            F.when(F.col("arr_movement_label") == "expansion_arr",   F.col("arr_delta_dollars")).otherwise(0)
        ).alias("expansion_arr_dollars"),
        F.sum(
            F.when(F.col("arr_movement_label") == "contraction_arr", F.col("arr_delta_dollars")).otherwise(0)
        ).alias("contraction_arr_dollars"),
        F.sum(
            F.when(F.col("arr_movement_label") == "churned_arr",     F.col("arr_delta_dollars")).otherwise(0)
        ).alias("churn_arr_dollars"),
        F.sum(
            F.when(F.col("arr_movement_label") == "retained_arr",    F.col("closing_arr_dollars")).otherwise(0)
        ).alias("retained_arr_dollars"),
        F.sum("closing_arr_dollars").alias("total_closing_arr_dollars"),
        F.sum("beginning_arr_dollars").alias("total_beginning_arr_dollars"),
    )
    return agg_df


def compute_cohort_nrr(df: "DataFrame") -> "DataFrame":
    """
    Compute Net Revenue Retention (NRR) for each cohort × period pair.

    NRR is defined as ending ARR of the original cohort (excluding new logos
    acquired after the cohort month) divided by beginning ARR.  Values above
    1.0 indicate net expansion; below 1.0 indicates net churn.
    """
    numerator = (
        F.col("total_beginning_arr_dollars")
        + F.col("expansion_arr_dollars")
        + F.col("contraction_arr_dollars")
        + F.col("churn_arr_dollars")
    )

    df = df.withColumn(
        "net_revenue_retention_rate",
        F.when(
            F.col("total_beginning_arr_dollars") > 0,
            F.round(numerator / F.col("total_beginning_arr_dollars"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    return df


def write_output(df: "DataFrame") -> None:
    (
        df.repartition(16, F.col("cohort_month"))
          .write
          .mode("overwrite")
          .partitionBy("cohort_month")
          .parquet(OUTPUT_PATH)
    )
    logger.info("Cohort ARR matrix written to %s", OUTPUT_PATH)


def main():
    args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc    = SparkContext()
    gc    = GlueContext(sc)
    spark = gc.spark_session
    job   = Job(gc)
    job.init(args["JOB_NAME"], args)

    events     = load_mrr_events(spark)
    with_cohort = derive_cohort_key(events)
    with_index  = compute_cohort_index(with_cohort)
    with_arr    = build_account_arr_snapshots(with_index)
    with_begin  = compute_beginning_arr(with_arr)
    with_labels = tag_expansion_contraction_churn(with_begin)
    matrix      = aggregate_cohort_matrix(with_labels)
    with_nrr    = compute_cohort_nrr(matrix)

    write_output(with_nrr)
    job.commit()
    logger.info("subscription_cohort_builder complete")


if __name__ == "__main__":
    main()
