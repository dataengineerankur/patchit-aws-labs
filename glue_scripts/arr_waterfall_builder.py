"""
AWS Glue PySpark Job: ARR Waterfall Builder

Terminal job in the SaaS Revenue Intelligence pipeline.  Reads NRR metrics,
the cohort matrix, and the raw MRR event ledger to construct the standard
ARR waterfall report used in board-level and investor reporting.

The waterfall decomposes period-over-period ARR change into:
  New Business  — ARR from first-time paying customers
  Expansion     — ARR uplift from existing customers (upsell/cross-sell)
  Contraction   — ARR reduction from existing customers (downsell)
  Churn         — ARR lost to cancellations
  Reactivation  — ARR recovered from previously churned customers

The job validates waterfall arithmetic before writing so that no corrupt
report reaches the data warehouse.

Input  (NRR metrics)   : s3://saas-datalake/processed/nrr_metrics/
Input  (cohort matrix) : s3://saas-datalake/processed/cohort_arr_matrix/
Input  (MRR events)    : s3://saas-datalake/processed/mrr_events/
Output                 : s3://saas-datalake/processed/arr_waterfall/
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
from pyspark.sql.types import DoubleType, StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("arr_waterfall_builder")

NRR_METRICS_PATH  = "s3://saas-datalake/processed/nrr_metrics/"
COHORT_MATRIX_PATH = "s3://saas-datalake/processed/cohort_arr_matrix/"
MRR_EVENTS_PATH   = "s3://saas-datalake/processed/mrr_events/"
OUTPUT_PATH       = "s3://saas-datalake/processed/arr_waterfall/"

WATERFALL_COMPONENTS = [
    "new_business_arr", "expansion_arr", "reactivation_arr",
    "contraction_arr", "churn_arr",
]
RECONCILIATION_TOLERANCE = 0.01


def load_inputs(spark: SparkSession):
    nrr_df    = spark.read.parquet(NRR_METRICS_PATH)
    cohort_df = spark.read.parquet(COHORT_MATRIX_PATH)
    mrr_df    = spark.read.parquet(MRR_EVENTS_PATH)

    logger.info(
        "Loaded inputs — NRR: %d rows, cohort: %d rows, MRR events: %d rows",
        nrr_df.count(), cohort_df.count(), mrr_df.count()
    )
    return nrr_df, cohort_df, mrr_df


def build_period_opening_arr(mrr_df: "DataFrame") -> "DataFrame":
    """
    Derive opening ARR for each billing period by summing all active
    subscription closing MRR at the end of the prior period × 12.
    """
    lag_window = Window.orderBy(F.col("billing_period").asc())

    period_closing = mrr_df.groupBy("billing_period").agg(
        F.sum("closing_mrr_dollars").alias("total_closing_mrr")
    )
    period_closing = period_closing.withColumn(
        "total_closing_arr",
        F.col("total_closing_mrr") * 12
    )
    period_closing = period_closing.withColumn(
        "opening_arr",
        F.lag(F.col("total_closing_arr"), 2).over(lag_window)
    )
    return period_closing


def build_waterfall_components(mrr_df: "DataFrame") -> "DataFrame":
    """
    Roll up MRR event movements to period-level waterfall components.
    Only movement types that affect ARR contribute to waterfall components.
    """
    movement_agg = mrr_df.groupBy("billing_period", "movement_type").agg(
        F.sum("net_new_mrr_dollars").alias("total_mrr_movement")
    )

    waterfall = movement_agg.groupBy("billing_period").pivot(
        "movement_type",
        ["new_business", "expansion", "contraction", "churned", "reactivated", "neutral"]
    ).agg(F.sum("total_mrr_movement"))

    rename_map = {
        "new_business":  "new_business_mrr",
        "expansion":     "expansion_mrr",
        "contraction":   "contraction_mrr",
        "churned":       "churn_mrr",
        "reactivated":   "reactivation_mrr",
        "neutral":       "neutral_mrr",
    }
    for old, new in rename_map.items():
        if old in waterfall.columns:
            waterfall = waterfall.withColumnRenamed(old, new)
        else:
            waterfall = waterfall.withColumn(new, F.lit(0.0).cast(DoubleType()))

    for col in ["new_business_mrr", "expansion_mrr", "contraction_mrr",
                "churn_mrr", "reactivation_mrr"]:
        waterfall = waterfall.withColumn(
            col.replace("_mrr", "_arr"),
            F.coalesce(F.col(col), F.lit(0.0)) * 12
        )
    return waterfall


def join_nrr_to_waterfall(waterfall_df: "DataFrame", nrr_df: "DataFrame") -> "DataFrame":
    """
    Attach period-level blended NRR and GRR to the waterfall for context.
    Uses a simple average of cohort NRRs weighted by beginning ARR.
    """
    blended_nrr = nrr_df.groupBy("billing_period").agg(
        F.sum(
            F.col("nrr_point_in_time") * F.col("total_beginning_arr_dollars")
        ).alias("nrr_weighted_sum"),
        F.sum("total_beginning_arr_dollars").alias("total_beginning_arr"),
        F.avg("grr_point_in_time").alias("avg_grr"),
        F.avg("nrr_rolling_12m").alias("avg_nrr_rolling_12m"),
    )
    blended_nrr = blended_nrr.withColumn(
        "blended_nrr",
        F.when(
            F.col("total_beginning_arr") > 0,
            F.round(F.col("nrr_weighted_sum") / F.col("total_beginning_arr"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    return waterfall_df.join(
        blended_nrr.select(
            "billing_period", "blended_nrr", "avg_grr",
            "avg_nrr_rolling_12m", "total_beginning_arr"
        ),
        on="billing_period",
        how="left"
    )


def compute_closing_arr(df: "DataFrame") -> "DataFrame":
    """
    closing_arr = opening_arr + new_business_arr + expansion_arr
                + reactivation_arr + contraction_arr + churn_arr

    contraction_arr and churn_arr should carry negative signs from the
    upstream mrr_event_processor movement classification.
    """
    df = df.withColumn(
        "waterfall_closing_arr",
        F.col("opening_arr")
        + F.coalesce(F.col("new_business_arr"),  F.lit(0.0))
        + F.coalesce(F.col("expansion_arr"),     F.lit(0.0))
        + F.coalesce(F.col("reactivation_arr"),  F.lit(0.0))
        + F.coalesce(F.col("contraction_arr"),   F.lit(0.0))
        + F.coalesce(F.col("churn_arr"),         F.lit(0.0))
    )
    return df


def reconcile_waterfall(df: "DataFrame") -> None:
    """
    Cross-check that the waterfall-derived closing ARR matches the directly
    measured closing ARR from the MRR ledger.  A mismatch beyond tolerance
    indicates mis-classification of MRR movements (e.g. sign errors in
    contraction/churn components, double-counting of reactivations).

    Tolerance is set at 1% of closing ARR — residuals above this threshold
    represent a material accounting discrepancy that must be resolved before
    the waterfall report is published.
    """
    mismatch_df = df.filter(
        F.col("opening_arr").isNotNull() &
        F.col("waterfall_closing_arr").isNotNull() &
        F.col("total_closing_arr").isNotNull()
    ).withColumn(
        "abs_delta",
        F.abs(F.col("waterfall_closing_arr") - F.col("total_closing_arr"))
    ).withColumn(
        "pct_delta",
        F.when(
            F.col("total_closing_arr") != 0,
            F.col("abs_delta") / F.abs(F.col("total_closing_arr"))
        ).otherwise(F.lit(0.0))
    ).filter(F.col("pct_delta") > RECONCILIATION_TOLERANCE)

    bad_periods = mismatch_df.count()
    if bad_periods > 0:
        mismatch_df.select(
            "billing_period",
            "opening_arr",
            "waterfall_closing_arr",
            "total_closing_arr",
            "abs_delta",
            "pct_delta",
        ).orderBy("pct_delta", ascending=False).show(10, truncate=False)

        worst = mismatch_df.agg(F.max("pct_delta").alias("max_pct")).first()["max_pct"]
        raise ValueError(
            f"ARR waterfall reconciliation failed: {bad_periods} period(s) "
            f"exceed tolerance — worst deviation {worst:.2%}. "
            f"Check movement_type sign conventions in mrr_event_processor, "
            f"NRR formula in net_revenue_retention_calculator, and contraction/"
            f"churn sign propagation through subscription_cohort_builder and "
            f"revenue_period_allocator."
        )

    logger.info("Waterfall reconciliation passed for all periods")


def compute_period_over_period_growth(df: "DataFrame") -> "DataFrame":
    """Add period-over-period ARR growth rate for trend analysis."""
    pop_window = Window.orderBy(F.col("billing_period").asc())
    df = df.withColumn(
        "prior_period_closing_arr",
        F.lag(F.col("total_closing_arr"), 1).over(pop_window)
    )
    df = df.withColumn(
        "arr_growth_rate",
        F.when(
            F.col("prior_period_closing_arr").isNotNull() &
            (F.col("prior_period_closing_arr") > 0),
            F.round(
                (F.col("total_closing_arr") - F.col("prior_period_closing_arr"))
                / F.col("prior_period_closing_arr"),
                4
            )
        ).otherwise(F.lit(None).cast(DoubleType()))
    )
    return df


def write_output(df: "DataFrame") -> None:
    output_cols = [
        "billing_period",
        "opening_arr", "total_closing_arr", "waterfall_closing_arr",
        "new_business_arr", "expansion_arr", "reactivation_arr",
        "contraction_arr", "churn_arr",
        "blended_nrr", "avg_grr", "avg_nrr_rolling_12m",
        "arr_growth_rate",
        "total_beginning_arr",
    ]
    available = [c for c in output_cols if c in df.columns]
    (
        df.select(available)
          .coalesce(4)
          .write
          .mode("overwrite")
          .parquet(OUTPUT_PATH)
    )
    logger.info("ARR waterfall written to %s", OUTPUT_PATH)


def main():
    args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc    = SparkContext()
    gc    = GlueContext(sc)
    spark = gc.spark_session
    job   = Job(gc)
    job.init(args["JOB_NAME"], args)

    nrr_df, cohort_df, mrr_df = load_inputs(spark)

    opening_df    = build_period_opening_arr(mrr_df)
    components_df = build_waterfall_components(mrr_df)

    waterfall_df  = opening_df.join(components_df, on="billing_period", how="outer")
    waterfall_df  = join_nrr_to_waterfall(waterfall_df, nrr_df)
    waterfall_df  = compute_closing_arr(waterfall_df)
    waterfall_df  = compute_period_over_period_growth(waterfall_df)

    reconcile_waterfall(waterfall_df)

    write_output(waterfall_df)
    job.commit()
    logger.info("arr_waterfall_builder complete")


if __name__ == "__main__":
    main()
