"""
AWS Glue PySpark Job: Net Revenue Retention Calculator

Consumes the cohort ARR matrix produced by subscription_cohort_builder and
the allocated revenue ledger from revenue_period_allocator to compute
monthly and rolling-12-month Net Revenue Retention (NRR) and Gross Revenue
Retention (GRR) metrics per cohort slice.

NRR definition:
    Ending ARR of the original cohort (expansions + contractions + churn)
    divided by beginning ARR of that same cohort in the measurement period.
    Excludes new logos acquired after the cohort's acquisition month.

GRR definition:
    Ending ARR excluding expansions (capped at 100%) divided by beginning ARR.

Outputs a fact table suitable for BI/dashboarding at month granularity.

Input  S3 path (cohort matrix): s3://saas-datalake/processed/cohort_arr_matrix/
Input  S3 path (allocated rev) : s3://saas-datalake/processed/allocated_revenue/
Output S3 path                 : s3://saas-datalake/processed/nrr_metrics/
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
from pyspark.sql.types import DoubleType, IntegerType, StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("net_revenue_retention_calculator")

COHORT_MATRIX_PATH  = "s3://saas-datalake/processed/cohort_arr_matrix/"
ALLOCATED_REV_PATH  = "s3://saas-datalake/processed/allocated_revenue/"
OUTPUT_PATH         = "s3://saas-datalake/processed/nrr_metrics/"

COHORT_SLICE_DIMS   = ["industry_vertical", "geo_region", "account_tier"]
ROLLING_WINDOW_MONTHS = 12


def load_cohort_matrix(spark: SparkSession) -> "DataFrame":
    df = spark.read.parquet(COHORT_MATRIX_PATH)
    logger.info("Loaded cohort matrix: %d rows", df.count())
    return df


def load_allocated_revenue(spark: SparkSession) -> "DataFrame":
    df = spark.read.parquet(ALLOCATED_REV_PATH)
    logger.info("Loaded allocated revenue: %d rows", df.count())
    return df


def compute_point_in_time_nrr(cohort_df: "DataFrame") -> "DataFrame":
    """
    For each cohort × period observation compute the point-in-time NRR.

    NRR = (beginning_arr + expansion + contraction + churn) / beginning_arr

    All four components should be signed correctly:
        expansion_arr_dollars   > 0
        contraction_arr_dollars < 0
        churn_arr_dollars       < 0  (negative, representing lost ARR)
    """
    ending_arr = (
        F.col("total_beginning_arr_dollars")
        + F.col("expansion_arr_dollars")
        + F.col("contraction_arr_dollars")
        + F.col("churn_arr_dollars")
    )

    cohort_df = cohort_df.withColumn(
        "nrr_point_in_time",
        F.when(
            F.col("total_beginning_arr_dollars") > 0,
            F.round(ending_arr / F.col("total_beginning_arr_dollars"), 6)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    return cohort_df


def compute_point_in_time_grr(cohort_df: "DataFrame") -> "DataFrame":
    """
    GRR excludes expansion — only contraction and churn reduce it.
    Capped at 1.0 (100%) since expansions cannot improve GRR.
    """
    grr_numerator = (
        F.col("total_beginning_arr_dollars")
        + F.col("contraction_arr_dollars")
        + F.col("churn_arr_dollars")
    )
    cohort_df = cohort_df.withColumn(
        "grr_point_in_time",
        F.when(
            F.col("total_beginning_arr_dollars") > 0,
            F.least(
                F.lit(1.0),
                F.round(grr_numerator / F.col("total_beginning_arr_dollars"), 6)
            )
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    return cohort_df


def compute_rolling_nrr(cohort_df: "DataFrame") -> "DataFrame":
    """
    Compute a trailing-12-month NRR for each cohort × slice by summing
    numerator and denominator components over the rolling window before
    dividing — avoids the averaging-of-ratios distortion.
    """
    roll_window = (
        Window.partitionBy(["cohort_month"] + COHORT_SLICE_DIMS)
              .orderBy(F.col("cohort_index").asc())
              .rowsBetween(-(ROLLING_WINDOW_MONTHS - 1), 0)
    )

    cohort_df = cohort_df.withColumn(
        "rolling_12m_beginning_arr",
        F.sum("total_beginning_arr_dollars").over(roll_window)
    )
    cohort_df = cohort_df.withColumn(
        "rolling_12m_expansion",
        F.sum("expansion_arr_dollars").over(roll_window)
    )
    cohort_df = cohort_df.withColumn(
        "rolling_12m_contraction",
        F.sum("contraction_arr_dollars").over(roll_window)
    )
    cohort_df = cohort_df.withColumn(
        "rolling_12m_churn",
        F.sum("churn_arr_dollars").over(roll_window)
    )

    rolling_ending_arr = (
        F.col("rolling_12m_beginning_arr")
        + F.col("rolling_12m_expansion")
        + F.col("rolling_12m_contraction")
        + F.col("rolling_12m_churn")
    )

    cohort_df = cohort_df.withColumn(
        "nrr_rolling_12m",
        F.when(
            (F.col("rolling_12m_beginning_arr") > 0) & (F.col("cohort_index") >= ROLLING_WINDOW_MONTHS - 1),
            F.round(rolling_ending_arr / F.col("rolling_12m_beginning_arr"), 6)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    return cohort_df


def compute_logo_retention(cohort_df: "DataFrame") -> "DataFrame":
    """
    Logo churn rate = accounts lost (churned) / beginning account count.
    Logo retention = 1 - logo churn.
    """
    cohort_df = cohort_df.withColumn(
        "churned_accounts",
        F.when(F.col("churn_arr_dollars") < 0, F.col("account_count")).otherwise(F.lit(0))
    )
    cohort_df = cohort_df.withColumn(
        "logo_retention_rate",
        F.when(
            F.col("account_count") > 0,
            F.round(
                1.0 - (F.col("churned_accounts") / F.col("account_count")),
                4
            )
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    return cohort_df


def join_recognised_revenue(cohort_df: "DataFrame", alloc_df: "DataFrame") -> "DataFrame":
    """
    Enrich each cohort × period row with total GAAP-recognised revenue
    from the allocated revenue ledger.  This enables ARR-to-revenue bridge
    analysis in downstream BI tools.
    """
    recognised = alloc_df.filter(
        F.col("recognition_status").isin("recognised", "current")
    ).groupBy(
        F.col("cohort_month"),
        F.col("allocated_period").alias("billing_period")
    ).agg(
        F.sum("recognised_revenue_dollars").alias("total_recognised_revenue")
    )

    return cohort_df.join(recognised, on=["cohort_month", "billing_period"], how="left")


def apply_data_quality_filters(df: "DataFrame") -> "DataFrame":
    """
    Remove statistical outliers and edge cases that would skew NRR reporting:
      - Cohorts with fewer than 3 accounts (too small for meaningful metrics)
      - Periods where beginning ARR is zero (acquisition month, no retention signal)
    """
    return df.filter(
        (F.col("account_count") >= 3) &
        (F.col("total_beginning_arr_dollars") > 0)
    )


def validate_nrr_plausibility(df: "DataFrame") -> None:
    """
    Sanity-check that NRR values fall within economically plausible ranges.
    NRR above 300% or below 0% indicates a data pipeline error upstream.
    """
    extremes = df.filter(
        F.col("nrr_point_in_time").isNotNull() &
        (
            (F.col("nrr_point_in_time") > 3.0) |
            (F.col("nrr_point_in_time") < 0.0)
        )
    )
    extreme_count = extremes.count()
    if extreme_count > 0:
        sample = extremes.select(
            "cohort_month", "billing_period", "nrr_point_in_time",
            "total_beginning_arr_dollars", "expansion_arr_dollars",
            "contraction_arr_dollars", "churn_arr_dollars"
        ).limit(5)
        sample.show(truncate=False)
        logger.warning("Found %d cohort-periods with implausible NRR — review upstream data", extreme_count)


def write_output(df: "DataFrame") -> None:
    output_cols = [
        "cohort_month", "billing_period", "cohort_index",
        "industry_vertical", "geo_region", "account_tier",
        "account_count", "churned_accounts",
        "total_beginning_arr_dollars", "total_closing_arr_dollars",
        "new_arr_dollars", "expansion_arr_dollars",
        "contraction_arr_dollars", "churn_arr_dollars",
        "nrr_point_in_time", "grr_point_in_time", "nrr_rolling_12m",
        "logo_retention_rate",
        "rolling_12m_beginning_arr", "rolling_12m_expansion",
        "rolling_12m_contraction", "rolling_12m_churn",
        "total_recognised_revenue",
    ]
    available = [c for c in output_cols if c in df.columns]
    (
        df.select(available)
          .repartition(8, F.col("cohort_month"))
          .write
          .mode("overwrite")
          .partitionBy("cohort_month")
          .parquet(OUTPUT_PATH)
    )
    logger.info("NRR metrics written to %s", OUTPUT_PATH)


def main():
    args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc    = SparkContext()
    gc    = GlueContext(sc)
    spark = gc.spark_session
    job   = Job(gc)
    job.init(args["JOB_NAME"], args)

    cohort_df = load_cohort_matrix(spark)
    alloc_df  = load_allocated_revenue(spark)

    df = compute_point_in_time_nrr(cohort_df)
    df = compute_point_in_time_grr(df)
    df = compute_rolling_nrr(df)
    df = compute_logo_retention(df)
    df = join_recognised_revenue(df, alloc_df)
    df = apply_data_quality_filters(df)

    validate_nrr_plausibility(df)

    write_output(df)
    job.commit()
    logger.info("net_revenue_retention_calculator complete")


if __name__ == "__main__":
    main()
