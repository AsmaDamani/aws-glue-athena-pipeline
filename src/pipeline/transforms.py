"""Pure PySpark transformations used by the Glue job.

Kept free of any AWS Glue imports so they can be unit-tested locally
with a plain SparkSession.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

RAW_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("user_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.StringType()),
        T.StructField("amount", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("ingested_at", T.StringType()),
    ]
)

VALID_EVENT_TYPES = ["view", "add_to_cart", "purchase", "refund"]


def standardise(df: DataFrame) -> DataFrame:
    """Trim strings, normalise casing and cast to proper types."""
    return (
        df.select(
            F.trim("event_id").alias("event_id"),
            F.trim("user_id").alias("user_id"),
            F.lower(F.trim("event_type")).alias("event_type"),
            F.to_timestamp("event_ts").alias("event_ts"),
            F.col("amount").cast(T.DecimalType(12, 2)).alias("amount"),
            F.upper(F.trim("country")).alias("country"),
            F.to_timestamp("ingested_at").alias("ingested_at"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep the most recently ingested copy of each event_id.

    Source systems often resend events; a plain dropDuplicates() would keep
    an arbitrary copy, so we rank by ingestion time instead.
    """
    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc_nulls_last())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


@dataclass(frozen=True)
class QualityResult:
    valid: DataFrame
    rejected: DataFrame


def apply_quality_rules(df: DataFrame) -> QualityResult:
    """Split rows into valid and rejected, recording why each row failed."""
    checks = F.array(
        F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), F.lit("missing_event_id")),
        F.when(F.col("user_id").isNull() | (F.col("user_id") == ""), F.lit("missing_user_id")),
        F.when(F.col("event_ts").isNull(), F.lit("bad_timestamp")),
        F.when(
            F.col("event_type").isNull() | ~F.col("event_type").isin(VALID_EVENT_TYPES),
            F.lit("unknown_event_type"),
        ),
        F.when(
            F.col("event_type").isin("purchase", "refund") & F.col("amount").isNull(),
            F.lit("missing_amount"),
        ),
    )
    # Each check yields a reason string or null; keep only the failures.
    reasons = F.filter(checks, lambda x: x.isNotNull())

    checked = df.withColumn("dq_reasons", reasons)
    valid = checked.filter(F.size("dq_reasons") == 0).drop("dq_reasons")
    rejected = checked.filter(F.size("dq_reasons") > 0)
    return QualityResult(valid=valid, rejected=rejected)


def daily_summary(df: DataFrame) -> DataFrame:
    """Aggregate valid events into a small table that is cheap to query."""
    return df.groupBy("event_date", "country").agg(
        F.countDistinct("user_id").alias("active_users"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.coalesce(
            F.sum(F.when(F.col("event_type") == "purchase", F.col("amount"))), F.lit(0)
        ).alias("revenue"),
        F.coalesce(
            F.sum(F.when(F.col("event_type") == "refund", F.col("amount"))), F.lit(0)
        ).alias("refunds"),
    )
