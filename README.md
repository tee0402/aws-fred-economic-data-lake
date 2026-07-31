# AWS FRED Economic Data Lake

A serverless AWS data pipeline that ingests 30 economic data series from the FRED API, transforms the data into an analytics-ready data lake, and makes it queryable with Athena.

## Architecture

```
EventBridge (Daily Schedule)
          │
          ▼
    Step Functions
          │
          ▼
    Lambda (Ingestion)
          │
          ▼
       S3 (Raw)
raw/ingestion_date=YYYY-MM-DD/
          │
          ▼
  Glue ETL (PySpark)
   • Transform
   • Data Validation (Glue Data Quality rules)
   • Write Parquet
          │
          ▼
    S3 (Processed)
processed/economic_indicators/
          │
          ▼
Glue Crawler (Asynchronous)
          │
          ▼
  Glue Data Catalog
          │
          ▼
        Athena


Failures (Lambda/Glue ETL/Start Crawler)
                │
                ▼
    Step Functions Catch Handler
                │
                ▼
            SNS Email
```

## Design Decisions

**Per-series failure isolation over all-or-nothing ingestion** — A series failing
to fetch (after retries on 5xx errors, timeouts, and connection errors) is simply
skipped and doesn't block the other series from being processed.

**Partitioning by `series_id`, not `date`** — The expected access pattern of this data
is that it will be queried by series, so partitioning by `series_id` lets Athena
prune directly to the relevant partition.

**Dynamic partition overwrite** — Spark's default static overwrite clears all
partitions before writing. Since per-series ingestion failures are expected
and tolerated, a static overwrite could silently delete some existing partitions
from previous successful runs without replacing them. By using
`spark.sql.sources.partitionOverwriteMode = dynamic`, only partitions of successfully
ingested series will be updated.

**Glue Data Quality evaluation embedded in ETL job, not run post-crawler** — Setting
up Glue Data Quality evaluation as a Step Functions state requires the Glue Crawler
to be run first to populate the Glue Data Catalog table so it can be referenced in
Glue Data Quality. However, this means that bad data will already be cataloged and
queryable in Athena by the time that the evaluation runs, which is not a desirable
behavior. By moving the evaluation directly into the ETL job, the pipeline can be
failed if any rule fails, and bad data is blocked from reaching the curated layer.

## EventBridge Daily Schedule

![EventBridge daily schedule](screenshots/eventbridge_schedule.png)

## Step Functions Orchestration

![Step Functions](screenshots/step_functions.png)

## Lambda Execution

![Lambda execution start](screenshots/lambda_start.png)
![Lambda execution end](screenshots/lambda_end.png)

## S3 Raw Data Lake

![S3 raw](screenshots/s3_raw.png)
[Sample JSON](sample_data/A191RL1Q225SBEA.json)

## Glue ETL Job Run

![Glue ETL job run](screenshots/glue_job_run.png)

## S3 Processed Data Lake

![S3 processed](screenshots/s3_processed.png)
![S3 processed series](screenshots/s3_processed_series.png)

## Glue Data Catalog Table

![Glue Data Catalog table](screenshots/glue_table.png)

## Athena Query

![Athena query](screenshots/athena_query.png)
[Sample results](sample_data/athena_results.csv)

[More sample queries](athena/sample_queries.sql)

## SNS Failure Email

![SNS failure email](screenshots/sns_failure_email.png)