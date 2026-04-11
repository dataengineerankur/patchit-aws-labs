"""
AWS Glue PySpark Job: Customer RFM Segmentation
Reads raw customer and order data from S3, computes RFM scores,
normalizes to quintile buckets, classifies segments, and writes
enriched output back to S3.
"""

import sys
import logging
from datetime import datetime, timedelta

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
    DoubleType, DateType, TimestampType, LongType
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("customer_rfm_segmentation")

# ---------------------------------------------------------------------------
# Job parameter resolution
# ---------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "customers_path",
        "orders_path",
        "output_path",
        "recency_weight",
        "frequency_weight",
        "monetary_weight",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger.info("Job parameters resolved: %s", {k: v for k, v in args.items() if k != "JOB_NAME"})

CUSTOMERS_PATH   = args["customers_path"]
ORDERS_PATH      = args["orders_path"]
OUTPUT_PATH      = args["output_path"]
RECENCY_WEIGHT   = float(args.get("recency_weight",   "0.3"))
FREQUENCY_WEIGHT = float(args.get("frequency_weight", "0.35"))
MONETARY_WEIGHT  = float(args.get("monetary_weight",  "0.35"))

assert abs(RECENCY_WEIGHT + FREQUENCY_WEIGHT + MONETARY_WEIGHT - 1.0) < 1e-6, \
    "RFM weights must sum to 1.0"

# ---------------------------------------------------------------------------
# Schema definitions
# ---------------------------------------------------------------------------
CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",  StringType(),  False),
    StructField("first_name",   StringType(),  True),
    StructField("last_name",    StringType(),  True),
    StructField("email",        StringType(),  True),
    StructField("signup_date",  StringType(),  True),
    StructField("country",      StringType(),  True),
    StructField("segment",      StringType(),  True),
])

ORDER_SCHEMA = StructType([
    StructField("order_id",      StringType(),  False),
    StructField("customer_id",   StringType(),  False),
    StructField("order_date",    StringType(),  True),
    StructField("status",        StringType(),  True),
    StructField("total_amount",  DoubleType(),  True),
])

# ---------------------------------------------------------------------------
# Utility: quintile bucketing (1 = worst, 5 = best)
# ---------------------------------------------------------------------------
def quintile_score(df, col_name, score_col, ascending=True):
    """
    Assigns a score of 1-5 using Spark's ntile(5) window function.
    For recency, lower days-since-last-order is better (ascending=False
    reverses the bucketing direction).
    """
    w = Window.orderBy(F.col(col_name).asc() if ascending else F.col(col_name).desc())
    df = df.withColumn("_ntile_raw", F.ntile(5).over(w))
    if ascending:
        df = df.withColumn(score_col, F.col("_ntile_raw"))
    else:
        df = df.withColumn(score_col, F.lit(6) - F.col("_ntile_raw"))
    return df.drop("_ntile_raw")


# ---------------------------------------------------------------------------
# Utility: segment classification from composite RFM score
# ---------------------------------------------------------------------------
def classify_segment(rfm_score_col):
    return (
        F.when(F.col(rfm_score_col) >= 4.5, "Champions")
         .when((F.col(rfm_score_col) >= 3.5) & (F.col(rfm_score_col) < 4.5), "Loyal")
         .when((F.col(rfm_score_col) >= 2.5) & (F.col(rfm_score_col) < 3.5), "At-Risk")
         .when((F.col(rfm_score_col) >= 1.5) & (F.col(rfm_score_col) < 2.5), "Lost")
         .otherwise("New")
    )


# ---------------------------------------------------------------------------
# STEP 1 – Load raw data via GlueContext (CSV with header)
# ---------------------------------------------------------------------------
logger.info("Reading customer data from: %s", CUSTOMERS_PATH)
customers_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [CUSTOMERS_PATH]},
    format="csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx="customers_source",
)
customers_df = customers_dyf.toDF()
customers_df = customers_df.withColumn(
    "signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd")
)
logger.info("Loaded %d customer records", customers_df.count())

logger.info("Reading order data from: %s", ORDERS_PATH)
orders_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [ORDERS_PATH]},
    format="csv",
    format_options={"withHeader": True, "separator": ","},
    transformation_ctx="orders_source",
)
orders_df = orders_dyf.toDF()
orders_df = (
    orders_df
    .withColumn("order_date",   F.to_date(F.col("order_date"),  "yyyy-MM-dd"))
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    .filter(F.col("status").isin("COMPLETED", "SHIPPED", "DELIVERED"))
)
logger.info("Loaded %d completed order records", orders_df.count())


# ---------------------------------------------------------------------------
# STEP 2 – Compute raw RFM metrics per customer
# ---------------------------------------------------------------------------
reference_date = orders_df.agg(F.max("order_date").alias("max_date")).collect()[0]["max_date"]
logger.info("Reference date (max order date in dataset): %s", reference_date)

rfm_raw = (
    orders_df
    .groupBy("customer_id")
    .agg(
        F.datediff(F.lit(reference_date), F.max("order_date")).alias("recency_days"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("total_amount").alias("monetary_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.avg("total_amount").alias("avg_order_value"),
        F.stddev("total_amount").alias("stddev_order_value"),
    )
)

# ---------------------------------------------------------------------------
# STEP 3 – Purchase velocity: average inter-order gap in days
# ---------------------------------------------------------------------------
@F.udf(returnType=DoubleType())
def compute_purchase_velocity(first_date, last_date, order_count):
    if first_date is None or last_date is None:
        return None
    total_span_days = (last_date - first_date).days
    avg_order_gap = total_span_days // (order_count - 1)
    velocity = round(1.0 / avg_order_gap, 6) if avg_order_gap > 0 else 0.0
    return float(velocity)


rfm_raw = rfm_raw.withColumn(
    "purchase_velocity",
    compute_purchase_velocity(
        F.col("first_order_date"),
        F.col("last_order_date"),
        F.col("order_count"),
    ),
)

# ---------------------------------------------------------------------------
# STEP 4 – Quintile scoring  (1-5 per dimension)
# ---------------------------------------------------------------------------
logger.info("Computing quintile scores …")

rfm_scored = quintile_score(rfm_raw,    "recency_days",   "r_score", ascending=False)
rfm_scored = quintile_score(rfm_scored, "order_count",    "f_score", ascending=True)
rfm_scored = quintile_score(rfm_scored, "monetary_value", "m_score", ascending=True)

# Composite weighted RFM score
rfm_scored = rfm_scored.withColumn(
    "rfm_composite",
    F.round(
        F.col("r_score") * F.lit(RECENCY_WEIGHT)
        + F.col("f_score") * F.lit(FREQUENCY_WEIGHT)
        + F.col("m_score") * F.lit(MONETARY_WEIGHT),
        4
    ),
)

# Concatenated RFM string for legacy reporting (e.g. "5-4-3")
rfm_scored = rfm_scored.withColumn(
    "rfm_cell",
    F.concat_ws("-",
        F.col("r_score").cast(StringType()),
        F.col("f_score").cast(StringType()),
        F.col("m_score").cast(StringType()),
    ),
)

# Segment label
rfm_scored = rfm_scored.withColumn("rfm_segment", classify_segment("rfm_composite"))

# ---------------------------------------------------------------------------
# STEP 5 – Enrich with customer master attributes
# ---------------------------------------------------------------------------
logger.info("Joining RFM scores with customer master data …")
enriched = customers_df.join(rfm_scored, on="customer_id", how="left")

# Customers who never placed a completed order
enriched = enriched.fillna(
    {
        "recency_days":      999,
        "order_count":       0,
        "monetary_value":    0.0,
        "avg_order_value":   0.0,
        "r_score":           1,
        "f_score":           1,
        "m_score":           1,
        "rfm_composite":     1.0,
        "rfm_cell":          "1-1-1",
        "rfm_segment":       "New",
        "purchase_velocity": 0.0,
    }
)

# Derived flags
enriched = enriched.withColumn(
    "is_high_value",
    F.when(F.col("rfm_composite") >= 4.0, True).otherwise(False)
)
enriched = enriched.withColumn(
    "churn_risk_score",
    F.round(
        F.lit(5.0) - F.col("rfm_composite")
        + (F.col("recency_days") / F.lit(365.0)) * F.lit(0.5),
        4
    ),
)
enriched = enriched.withColumn(
    "ltv_estimate",
    F.round(
        F.col("monetary_value")
        * (F.col("rfm_composite") / F.lit(5.0))
        * F.lit(1.2),  # retention multiplier
        2
    ),
)
enriched = enriched.withColumn(
    "processing_timestamp", F.current_timestamp()
)

# ---------------------------------------------------------------------------
# STEP 6 – Data quality checks before write
# ---------------------------------------------------------------------------
total_rows      = enriched.count()
null_segment    = enriched.filter(F.col("rfm_segment").isNull()).count()
negative_ltv    = enriched.filter(F.col("ltv_estimate") < 0).count()
logger.info("Total enriched rows : %d", total_rows)
logger.info("Null rfm_segment    : %d", null_segment)
logger.info("Negative ltv rows   : %d", negative_ltv)

if null_segment / max(total_rows, 1) > 0.05:
    raise RuntimeError(
        f"Data quality failure: {null_segment} of {total_rows} rows have null rfm_segment "
        f"({100 * null_segment / total_rows:.1f}% > 5% threshold)"
    )

# ---------------------------------------------------------------------------
# STEP 7 – Write enriched output
# ---------------------------------------------------------------------------
logger.info("Writing RFM segments to: %s", OUTPUT_PATH)

output_cols = [
    "customer_id", "first_name", "last_name", "email",
    "signup_date", "country",
    "recency_days", "order_count", "monetary_value",
    "avg_order_value", "stddev_order_value",
    "first_order_date", "last_order_date",
    "purchase_velocity",
    "r_score", "f_score", "m_score",
    "rfm_composite", "rfm_cell", "rfm_segment",
    "is_high_value", "churn_risk_score", "ltv_estimate",
    "processing_timestamp",
]

enriched_output = enriched.select(output_cols)

enriched_dyf = DynamicFrame.fromDF(enriched_output, glueContext, "enriched_rfm")

glueContext.write_dynamic_frame.from_options(
    frame=enriched_dyf,
    connection_type="s3",
    connection_options={
        "path": OUTPUT_PATH,
        "partitionKeys": ["rfm_segment"],
    },
    format="parquet",
    format_options={"compression": "snappy"},
    transformation_ctx="rfm_segments_sink",
)

logger.info("RFM segmentation job completed successfully. Rows written: %d", total_rows)

# ---------------------------------------------------------------------------
# Segment distribution summary (written to job log for observability)
# ---------------------------------------------------------------------------
segment_summary = (
    enriched_output
    .groupBy("rfm_segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.avg("monetary_value"), 2).alias("avg_monetary"),
        F.round(F.avg("rfm_composite"), 3).alias("avg_rfm_score"),
        F.round(F.avg("churn_risk_score"), 3).alias("avg_churn_risk"),
    )
    .orderBy(F.col("avg_rfm_score").desc())
)

logger.info("=== Segment Distribution ===")
for row in segment_summary.collect():
    logger.info(
        "  %-12s  customers=%5d  avg_monetary=%8.2f  avg_rfm=%.3f  churn_risk=%.3f",
        row["rfm_segment"], row["customer_count"],
        row["avg_monetary"], row["avg_rfm_score"], row["avg_churn_risk"],
    )

job.commit()
logger.info("Glue job committed.")
