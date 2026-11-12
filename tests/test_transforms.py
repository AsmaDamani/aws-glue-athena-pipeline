from decimal import Decimal
from pathlib import Path

import pytest

from pipeline.transforms import (
    RAW_SCHEMA,
    apply_quality_rules,
    daily_summary,
    deduplicate,
    standardise,
)

SAMPLE = str(Path(__file__).resolve().parents[1] / "sample_data" / "events.csv")


@pytest.fixture(scope="module")
def clean(spark):
    raw = spark.read.csv(SAMPLE, header=True, schema=RAW_SCHEMA)
    return deduplicate(standardise(raw)).cache()


def test_standardise_normalises_case(clean):
    row = clean.filter("event_id = 'e3'").first()
    assert row.event_type == "add_to_cart"
    assert row.country == "NZ"


def test_deduplicate_keeps_latest_copy(clean):
    dupes = clean.filter("event_id = 'e2'").selectExpr(
        "date_format(ingested_at, 'yyyy-MM-dd HH:mm:ss') AS ingested"
    ).collect()
    assert len(dupes) == 1
    assert dupes[0].ingested == "2026-11-01 10:00:00"


def test_quality_rules_split_rows(clean):
    result = apply_quality_rules(clean)
    rejected = {r.event_id: set(r.dq_reasons) for r in result.rejected.collect()}

    assert rejected["e4"] == {"missing_amount"}
    assert rejected["e5"] == {"missing_user_id"}
    assert rejected["e6"] == {"unknown_event_type"}
    assert rejected["e7"] == {"bad_timestamp"}

    valid_ids = {r.event_id for r in result.valid.collect()}
    assert valid_ids == {"e1", "e2", "e3", "e8", "e9"}
    assert valid_ids.isdisjoint(rejected)


def test_daily_summary(clean):
    summary = {
        (str(r.event_date), r.country): r
        for r in daily_summary(apply_quality_rules(clean).valid).collect()
    }
    au_day1 = summary[("2026-11-01", "AU")]
    assert au_day1.purchases == 1
    assert au_day1.revenue == Decimal("49.90")

    au_day2 = summary[("2026-11-02", "AU")]
    assert au_day2.purchases == 1
    assert au_day2.revenue == Decimal("120.00")
    assert au_day2.refunds == Decimal("10.00")
