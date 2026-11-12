"""AWS Glue job: raw events (CSV in S3) -> cleaned, partitioned Parquet + daily summary.

Job parameters (set in Terraform as default_arguments):
  --RAW_PATH        s3://<raw-bucket>/events/
  --CURATED_PATH    s3://<curated-bucket>/
  --DATABASE        Glue Data Catalog database name
  --extra-py-files  s3://<artifacts-bucket>/pipeline.zip  (the src/pipeline package)
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from pipeline.transforms import (
    RAW_SCHEMA,
    apply_quality_rules,
    daily_summary,
    deduplicate,
    standardise,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "JOB_RUN_ID", "RAW_PATH", "CURATED_PATH", "DATABASE"])

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job = Job(glue)
job.init(args["JOB_NAME"], args)

# Overwrite only the partitions present in this run, not the whole table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

curated = args["CURATED_PATH"].rstrip("/")
db = args["DATABASE"]

raw = spark.read.csv(args["RAW_PATH"], header=True, schema=RAW_SCHEMA)

clean = deduplicate(standardise(raw))
result = apply_quality_rules(clean)


def write_partitioned(df, table, path, mode="overwrite"):
    """Create the table on first run; afterwards insert so that, with dynamic
    partition overwrite, a re-run replaces only the dates it contains.
    (saveAsTable in overwrite mode would replace the whole table.)"""
    # insertInto matches columns by position and partition columns come last,
    # so put event_date at the end on every write.
    df = df.select(*[c for c in df.columns if c != "event_date"], "event_date")
    full_name = f"{db}.{table}"
    if spark.catalog.tableExists(full_name):
        df.write.insertInto(full_name, overwrite=(mode == "overwrite"))
    else:
        (
            df.write.mode(mode)
            .partitionBy("event_date")
            .format("parquet")
            .option("path", path)
            .saveAsTable(full_name)
        )


# Valid events: Parquet, partitioned by date so Athena scans only what it needs.
# Repartitioning by the partition key avoids many small files per date.
write_partitioned(result.valid.repartition("event_date"), "events", f"{curated}/events/")

# Rejected rows are kept, not dropped, so problems can be investigated.
write_partitioned(
    result.rejected.withColumn("run_id", F.lit(args["JOB_RUN_ID"])),
    "rejected_events",
    f"{curated}/rejected_events/",
    mode="append",
)

write_partitioned(
    daily_summary(result.valid).repartition("event_date"),
    "daily_summary",
    f"{curated}/daily_summary/",
)

job.commit()
