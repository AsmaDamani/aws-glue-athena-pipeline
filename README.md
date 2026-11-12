# AWS Glue + Athena data pipeline

A small, production-style batch pipeline on AWS: raw event files land in S3, a PySpark job on AWS Glue cleans and validates them, and the results are queryable in Athena as partitioned Parquet tables. Everything is deployed with Terraform.

It's a reference implementation of the patterns I use on client work. It's deliberately small, so the whole thing can be read in one sitting.

```
 S3 (raw CSV)  ──▶  Glue job (PySpark)  ──▶  S3 (curated Parquet)  ──▶  Athena
                     │  standardise                 events/            (workgroup with
                     │  deduplicate                 daily_summary/      scan limit)
                     │  data-quality rules          rejected_events/
                     ▼
            EventBridge ──▶ SNS email on job failure
```

## What it shows

| Concern | How it's handled |
|---|---|
| **Duplicate events** | Window function keeps the most recently ingested copy of each `event_id`, not an arbitrary one |
| **Bad data** | Rows failing rules are written to `rejected_events` with the reasons, never silently dropped |
| **Query cost** | Parquet, partitioned by `event_date`; Athena workgroup caps bytes scanned per query |
| **Small files** | Repartition by partition key before writing |
| **Re-runs** | Dynamic partition overwrite: a re-run replaces only the dates it touched |
| **Failures** | EventBridge rule on Glue job state → SNS email |
| **Testability** | All transforms are plain PySpark functions in `src/pipeline`, unit-tested locally with no AWS account |
| **Security** | Buckets block public access; the Glue role is read-only on raw data and code, with write access only to the curated bucket |

## Layout

```
src/pipeline/transforms.py     # pure PySpark logic (tested)
src/glue_jobs/clean_events.py  # thin Glue entry point
tests/                         # pytest with a local SparkSession
sample_data/events.csv         # includes duplicates and deliberately bad rows
infra/terraform/               # S3, IAM, Glue job + schedule, Athena, alerting
sql/example_queries.sql        # sample Athena queries
```

## Run the tests locally

Requires Java 11+ and Python 3.10+.

```bash
pip install -r requirements-dev.txt
make test
```

## Deploy

```bash
make package                         # zips src/pipeline for --extra-py-files
cd infra/terraform
terraform init
terraform apply -var="alert_email=you@example.com"

aws s3 cp ../../sample_data/events.csv s3://$(terraform output -raw raw_bucket)/events/
aws glue start-job-run --job-name $(terraform output -raw glue_job_name)
```

Then query the tables in the Athena workgroup from `terraform output athena_workgroup`, using the examples in `sql/`.

## Adapting it

- **Other sources:** swap the CSV read for JDBC, JSON or an API landing step. The transforms don't change.
- **Orchestration:** for multi-job dependencies, replace the Glue trigger with Airflow (MWAA) or Step Functions.
- **Incremental loads:** enable Glue job bookmarks or filter on `ingested_at`.

---

Built by Asma Nafees, data engineer (Python, PySpark, AWS). Available for data pipeline work on Upwork.
