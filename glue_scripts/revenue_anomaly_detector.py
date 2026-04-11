"""
AWS Glue PySpark Job: Revenue Anomaly Detector
Reads RFM-enriched customer segments and daily revenue rollup from S3,
joins them, computes rolling statistical baselines (7-day and 30-day moving
averages, z-scores, Bollinger Bands), and emits anomaly alert records.
"""

import sys
import logging
import math
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType, LongType
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("revenue_anomaly_detector")

# ---------------------------------------------------------------------------
# Job parameters
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "rfm_segments_path",
        "revenue_daily_path",
        "alerts_output_path",
        "zscore_threshold",
        "bollinger_std_multiplier",
        "lookback_days",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Initialised job: %s", args["JOB_NAME"])

RFM_SEGMENTS_PATH       = args["rfm_segments_path"]
REVENUE_DAILY_PATH      = args["revenue_daily_path"]
ALERTS_OUTPUT_PATH      = args["alerts_output_path"]
ZSCORE_THRESHOLD        = float(args.get("zscore_threshold",          "2.5"))
BOLLINGER_MULTIPLIER    = float(args.get("bollinger_std_multiplier",  "2.0"))
LOOKBACK_DAYS           = int(args.get("lookback_days",               "30"))

logger.info(
    "Config — z_threshold=%.2f  bollinger_mult=%.2f  lookback=%d",
    ZSCORE_THRESHOLD, BOLLINGER_MULTIPLIER, LOOKBACK_DAYS
)

# ---------------------------------------------------------------------------
# Schema: daily revenue rollup
# ---------------------------------------------------------------------------
REVENUE_SCHEMA = StructType([
    StructField("date",              StringType(),  False),
    StructField("segment",           StringType(),  False),
    StructField("revenue_amount",    DoubleType(),  True),
    StructField("transaction_count", IntegerType(), True),
    StructField("avg_basket_size",   DoubleType(),  True),
])

# ---------------------------------------------------------------------------
# STEP 1 – Load daily revenue
# ---------------------------------------------------------------------------
logger.info("Reading daily revenue data from: %s", REVENUE_DAILY_PATH)
revenue_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [REVENUE_DAILY_PATH]},
    format="csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx="revenue_daily_source",
)
revenue_df = revenue_dyf.toDF()
revenue_df = (
    revenue_df
    .withColumn("date",              F.to_date(F.col("date"), "yyyy-MM-dd"))
    .withColumn("revenue_amount",    F.col("revenue_amount").cast(DoubleType()))
    .withColumn("transaction_count", F.col("transaction_count").cast(IntegerType()))
    .withColumn("avg_basket_size",   F.col("avg_basket_size").cast(DoubleType()))
    .filter(F.col("date").isNotNull())
    .filter(F.col("revenue_amount") >= 0)
)
logger.info("Loaded %d daily revenue records", revenue_df.count())

# ---------------------------------------------------------------------------
# STEP 2 – Load RFM segment summary for enrichment
# ---------------------------------------------------------------------------
logger.info("Reading RFM segments from: %s", RFM_SEGMENTS_PATH)
rfm_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [RFM_SEGMENTS_PATH]},
    format="parquet",
    transformation_ctx="rfm_segments_source",
)
rfm_df = rfm_dyf.toDF()

# Build per-segment aggregate metrics for context enrichment
segment_metrics = (
    rfm_df
    .groupBy("rfm_segment")
    .agg(
        F.count("customer_id").alias("segment_customer_count"),
        F.round(F.avg("monetary_value"), 2).alias("segment_avg_monetary"),
        F.round(F.avg("rfm_composite"), 3).alias("segment_avg_rfm"),
        F.round(F.avg("churn_risk_score"), 3).alias("segment_avg_churn_risk"),
        F.sum("monetary_value").alias("segment_total_ltv"),
    )
    .withColumnRenamed("rfm_segment", "segment")
)
logger.info("Computed segment metrics for %d segments", segment_metrics.count())

# ---------------------------------------------------------------------------
# STEP 3 – Rolling windows for statistical baseline
# ---------------------------------------------------------------------------
date_rank_window = (
    Window
    .partitionBy("segment")
    .orderBy(F.col("date").cast("long"))
)

rolling_7_window = (
    Window
    .partitionBy("segment")
    .orderBy(F.col("date").cast("long"))
    .rowsBetween(-6, 0)          # inclusive of current row = 7 rows
)

rolling_30_window = (
    Window
    .partitionBy("segment")
    .orderBy(F.col("date").cast("long"))
    .rowsBetween(-29, 0)         # inclusive of current row = 30 rows
)

revenue_windowed = (
    revenue_df
    .withColumn("row_num", F.row_number().over(date_rank_window))

    # 7-day rolling stats
    .withColumn("ma_7d",    F.round(F.avg("revenue_amount").over(rolling_7_window), 4))
    .withColumn("std_7d",   F.round(F.stddev("revenue_amount").over(rolling_7_window), 4))
    .withColumn("min_7d",   F.min("revenue_amount").over(rolling_7_window))
    .withColumn("max_7d",   F.max("revenue_amount").over(rolling_7_window))

    # 30-day rolling stats
    .withColumn("ma_30d",   F.round(F.avg("revenue_amount").over(rolling_30_window), 4))
    .withColumn("std_30d",  F.round(F.stddev("revenue_amount").over(rolling_30_window), 4))
    .withColumn("min_30d",  F.min("revenue_amount").over(rolling_30_window))
    .withColumn("max_30d",  F.max("revenue_amount").over(rolling_30_window))

    # Momentum: percentage change vs prior day
    .withColumn("prev_day_revenue",
        F.lag("revenue_amount", 1).over(date_rank_window))
    .withColumn("pct_change_1d",
        F.round(
            (F.col("revenue_amount") - F.col("prev_day_revenue"))
            / F.when(F.col("prev_day_revenue") != 0, F.col("prev_day_revenue")).otherwise(F.lit(1.0))
            * F.lit(100.0),
            4
        )
    )

    # 7-day momentum direction (number of consecutive up-days)
    .withColumn("tx_count_7d_avg",
        F.round(F.avg("transaction_count").over(rolling_7_window), 2))
    .withColumn("basket_7d_avg",
        F.round(F.avg("avg_basket_size").over(rolling_7_window), 4))
)

# ---------------------------------------------------------------------------
# STEP 4 – Z-score computation and Bollinger Bands
# ---------------------------------------------------------------------------
revenue_scored = (
    revenue_windowed

    # Z-score against 30-day baseline
    .withColumn(
        "zscore_30d",
        F.round(
            F.when(
                F.col("std_30d").isNotNull() & (F.col("std_30d") > 0),
                (F.col("revenue_amount") - F.col("ma_30d")) / F.col("std_30d")
            ).otherwise(F.lit(0.0)),
            4
        )
    )

    # Bollinger Bands (upper and lower)
    .withColumn(
        "bollinger_upper",
        F.round(F.col("ma_30d") + F.lit(BOLLINGER_MULTIPLIER) * F.col("std_30d"), 4)
    )
    .withColumn(
        "bollinger_lower",
        F.round(F.col("ma_30d") - F.lit(BOLLINGER_MULTIPLIER) * F.col("std_30d"), 4)
    )

    # Boolean anomaly flags
    .withColumn(
        "is_zscore_anomaly",
        F.abs(F.col("zscore_30d")) > F.lit(ZSCORE_THRESHOLD)
    )
    .withColumn(
        "is_bollinger_breach",
        (F.col("revenue_amount") > F.col("bollinger_upper"))
        | (F.col("revenue_amount") < F.col("bollinger_lower"))
    )
    .withColumn(
        "is_anomaly",
        F.col("is_zscore_anomaly") | F.col("is_bollinger_breach")
    )
)

# ---------------------------------------------------------------------------
# STEP 5 – Anomaly severity scoring UDF
# ---------------------------------------------------------------------------
@F.udf(returnType=DoubleType())
def compute_anomaly_severity(row_dict_repr, zscore, pct_change, is_zscore_flag, is_bollinger_flag):
    """
    Assigns a continuous severity score in [0, 10].
    Combines z-score magnitude, momentum, and flag combination.
    """
    if zscore is None:
        return 0.0

    base_severity = min(abs(zscore) / ZSCORE_THRESHOLD * 4.0, 6.0)

    momentum_penalty = 0.0
    if pct_change is not None and abs(pct_change) > 20.0:
        momentum_penalty = min((abs(pct_change) - 20.0) / 10.0, 2.0)

    dual_flag_bonus = 2.0 if (is_zscore_flag and is_bollinger_flag) else 0.0

    return round(min(base_severity + momentum_penalty + dual_flag_bonus, 10.0), 4)


def build_anomaly_records(revenue_df_scored):
    """
    Materialises severity scores for ALL rows; filters to anomalies
    afterwards so the UDF runs in a single pass.
    """
    with_severity = revenue_df_scored.withColumn(
        "severity_score",
        compute_anomaly_severity(
            F.col("revenue_amount"),    # passed as positional placeholder
            F.col("zscore_30d"),
            F.col("pct_change_1d"),
            F.col("is_zscore_anomaly"),
            F.col("is_bollinger_breach"),
        )
    )

    anomalies = with_severity.filter(F.col("is_anomaly") == True)

    # Severity band label
    anomalies = anomalies.withColumn(
        "severity_band",
        F.when(F.col("severity_score") >= 8.0, "CRITICAL")
         .when(F.col("severity_score") >= 5.0, "HIGH")
         .when(F.col("severity_score") >= 3.0, "MEDIUM")
         .otherwise("LOW")
    )

    # Alert direction
    anomalies = anomalies.withColumn(
        "anomaly_direction",
        F.when(F.col("zscore_30d") > 0, "SPIKE").otherwise("DIP")
    )

    # Revenue deviation from 30-day mean (absolute and relative)
    anomalies = anomalies.withColumn(
        "revenue_deviation_abs",
        F.round(F.col("revenue_amount") - F.col("ma_30d"), 4)
    )
    anomalies = anomalies.withColumn(
        "revenue_deviation_pct",
        F.round(
            (F.col("revenue_amount") - F.col("ma_30d"))
            / F.when(F.col("ma_30d") != 0, F.col("ma_30d")).otherwise(F.lit(1.0))
            * F.lit(100.0),
            4
        )
    )

    # Reference revenue for audit trail
    anomalies = anomalies.withColumn(
        "reference_revenue_usd",
        F.col("revenue_amount")
    )

    anomalies = anomalies.withColumn(
        "alert_generated_at", F.current_timestamp()
    )
    return anomalies


logger.info("Building anomaly records …")
anomaly_records = build_anomaly_records(revenue_scored)

# ---------------------------------------------------------------------------
# STEP 6 – Join with segment metrics for enrichment context
# ---------------------------------------------------------------------------
alerts_enriched = anomaly_records.join(segment_metrics, on="segment", how="left")

# ---------------------------------------------------------------------------
# STEP 7 – Data quality gate on alert volume
# ---------------------------------------------------------------------------
total_revenue_rows = revenue_df.count()
total_anomalies    = alerts_enriched.count()
anomaly_rate       = total_anomalies / max(total_revenue_rows, 1)
logger.info(
    "Anomaly detection stats — total_rows=%d  anomalies=%d  rate=%.2f%%",
    total_revenue_rows, total_anomalies, anomaly_rate * 100
)

if anomaly_rate > 0.40:
    logger.warning(
        "Anomaly rate %.1f%% exceeds 40%% — possible baseline drift or data issue",
        anomaly_rate * 100
    )

# ---------------------------------------------------------------------------
# STEP 8 – Write alert records
# ---------------------------------------------------------------------------
alert_output_cols = [
    "date", "segment",
    "revenue_amount", "transaction_count", "avg_basket_size",
    "ma_7d", "std_7d", "ma_30d", "std_30d",
    "zscore_30d", "bollinger_upper", "bollinger_lower",
    "pct_change_1d",
    "is_zscore_anomaly", "is_bollinger_breach", "is_anomaly",
    "severity_score", "severity_band", "anomaly_direction",
    "revenue_deviation_abs", "revenue_deviation_pct",
    "reference_revenue_usd",
    "segment_customer_count", "segment_avg_monetary", "segment_avg_rfm",
    "alert_generated_at",
]

alerts_final = alerts_enriched.select(alert_output_cols)

logger.info("Writing anomaly alerts to: %s", ALERTS_OUTPUT_PATH)
alerts_dyf = DynamicFrame.fromDF(alerts_final, glueContext, "revenue_alerts")

glueContext.write_dynamic_frame.from_options(
    frame=alerts_dyf,
    connection_type="s3",
    connection_options={
        "path": ALERTS_OUTPUT_PATH,
        "partitionKeys": ["severity_band", "segment"],
    },
    format="parquet",
    format_options={"compression": "snappy"},
    transformation_ctx="revenue_alerts_sink",
)

logger.info("Anomaly detection job completed. Alerts written: %d", total_anomalies)

# ---------------------------------------------------------------------------
# Summary log by severity band
# ---------------------------------------------------------------------------
if total_anomalies > 0:
    severity_summary = (
        alerts_final
        .groupBy("severity_band", "anomaly_direction")
        .agg(
            F.count("*").alias("alert_count"),
            F.round(F.avg("severity_score"), 3).alias("avg_severity"),
            F.round(F.avg("revenue_deviation_pct"), 2).alias("avg_deviation_pct"),
        )
        .orderBy(F.col("avg_severity").desc())
    )
    logger.info("=== Anomaly Alert Summary ===")
    for row in severity_summary.collect():
        logger.info(
            "  %-8s  %-5s  count=%3d  avg_severity=%.3f  avg_dev_pct=%+.2f%%",
            row["severity_band"], row["anomaly_direction"],
            row["alert_count"], row["avg_severity"], row["avg_deviation_pct"]
        )

job.commit()
logger.info("Glue job committed.")
