import logging
import boto3
import os
import datetime
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception, before_sleep_log
import json
import time

# ── Setup ──────────────────────────────────────────────────────────────────────

log = logging.getLogger()
log.setLevel(logging.INFO)

secrets = boto3.client("secretsmanager")
s3 = boto3.client("s3")

# ── Config ──────────────────────────────────────────────────────────────────────

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
BUCKET_NAME = os.environ.get("BUCKET_NAME")
RAW_PATH = f"raw/ingestion_date={datetime.datetime.now().strftime('%Y-%m-%d')}/"

# ── FRED API ──────────────────────────────────────────────────────────────────────

def is_retryable(exception: Exception) -> bool:
    """Return True for 5xx server errors, timeouts, and connection errors."""
    if isinstance(exception, requests.exceptions.HTTPError):
        return exception.response.status_code >= 500
    if isinstance(exception, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return True
    return False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception(is_retryable),
    before_sleep=before_sleep_log(log, logging.WARNING)
)
def fetch_series(api_key: str, series_id: str) -> dict:
    """Fetch all observations for a single FRED series with 3 retries using exponential backoff for retryable errors."""
    log.info(f"Fetching {series_id}")

    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json"
    }

    response = requests.get(FRED_BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    
    data = response.json()
    data["series_id"] = series_id  # Add series_id to the data for easier identification
    log.info(f"Observations fetched from {series_id}")
    return data


# ── AWS ──────────────────────────────────────────────────────────────────────

def get_secret(secret_name: str) -> str:
    """Retrieve a secret from AWS Secrets Manager."""
    response = secrets.get_secret_value(SecretId=secret_name)
    secret = json.loads(response.get("SecretString"))
    return secret[secret_name]


def get_series_ids(key: str) -> list:
    """Retrieve the list of series IDs from S3."""
    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    series_ids = json.loads(response["Body"].read())
    log.info(f"Retrieved {len(series_ids)} series IDs from s3://{BUCKET_NAME}/{key}")
    return series_ids


def upload_to_s3(key: str, data: dict) -> None:
    """Upload a JSON file to S3."""
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(data))
    log.info(f"Uploaded JSON file to s3://{BUCKET_NAME}/{key}")


# ── Lambda Handler ──────────────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    log.info("Starting FRED ingestion")

    FRED_API_KEY = get_secret("FRED_API_KEY")
    SERIES_IDS = get_series_ids("config/series.json")

    # Validate environment
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY is not set in Secrets Manager")
    if not SERIES_IDS:
        raise ValueError("SERIES_IDS is not set in S3 config")
    if not BUCKET_NAME:
        raise ValueError("BUCKET_NAME is not set in Lambda environment variables")

    # Load new data to S3
    for series_id in SERIES_IDS:
        try:
            observations = fetch_series(FRED_API_KEY, series_id)
        except Exception as e:
            log.error(f"{series_id}: skipping after all retries failed - {e}")
            continue
        try:
            key = f"{RAW_PATH}{series_id}.json"
            upload_to_s3(key, observations)
        except Exception as e:
            log.error(f"{series_id}: failed to upload to S3 - {e}")
        time.sleep(0.5) # Wait 0.5s for politeness

    log.info("FRED ingestion complete")

    return {
        "status": "SUCCESS",
        "raw_path": f"s3://{BUCKET_NAME}/{RAW_PATH}"
    }