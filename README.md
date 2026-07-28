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
      • Data Validation
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

## EventBridge Daily Schedule

![EventBridge daily schedule](screenshots/eventbridge_schedule.png)

## Step Functions Orchestration

![Step Functions](screenshots/step_functions.png)

## Lambda Execution

![Lambda execution start](screenshots/lambda_start.png)
![Lambda execution end](screenshots/lambda_end.png)

## S3 Raw Data Lake

![S3 raw](screenshots/s3_raw.png)

## Glue ETL Job Run

![Glue ETL job run](screenshots/glue_job_run.png)

## S3 Processed Data Lake

![S3 processed](screenshots/s3_processed.png)
![S3 processed series](screenshots/s3_processed_series.png)

## Glue Data Catalog Table

![Glue Data Catalog table](screenshots/glue_table.png)

## Athena Query

![Athena query](screenshots/athena_query.png)

## SNS Failure Email

![SNS failure email](screenshots/sns_failure_email.png)