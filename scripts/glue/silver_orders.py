from __future__ import annotations

# AWS Glue job for processing orders data from bronze to silver layer
# This script is part of the DMS data lake orchestration pipeline

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters (with defaults for local testing)
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    job.init(args['JOB_NAME'], args)
except Exception:
    # Fallback for local/test execution
    args = {'JOB_NAME': 'dms-glue-silver-orders'}

# Define orders data - read from bronze layer
# This resolves the NameError: name 'orders' is not defined
try:
    orders = glueContext.create_dynamic_frame.from_catalog(
        database="bronze",
        table_name="orders"
    )
except Exception as e:
    # Fallback: create empty DynamicFrame if catalog table doesn't exist
    print(f"Warning: Could not read from catalog: {e}")
    orders = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": ["s3://dms-data-lake-bronze/orders/"]
        },
        format="parquet"
    )

# Process orders data for silver layer
# Apply any necessary transformations
orders_silver = orders

# Write to silver layer
glueContext.write_dynamic_frame.from_options(
    frame=orders_silver,
    connection_type="s3",
    connection_options={
        "path": "s3://dms-data-lake-silver/orders/"
    },
    format="parquet"
)

job.commit()
