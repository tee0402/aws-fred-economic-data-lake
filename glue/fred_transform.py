import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from awsgluedq.transforms import EvaluateDataQuality
from urllib.parse import urlparse

# ── Setup ──────────────────────────────────────────────────────────────────────

# Initialize Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("Starting Glue ETL job")

# Get the raw_path argument from the command line
args = getResolvedOptions(sys.argv, ["raw_path"])
raw_path = args["raw_path"]

# Use dynamic partition overwrite so failed series do not remove existing partitions from previous successful runs
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ── Transform ──────────────────────────────────────────────────────────────────────

print(f"Reading raw data from {raw_path}")

# Read the raw JSON data in S3 from all series for the specified ingestion date
df = spark.read.json(raw_path)

print("Successfully read raw data into DataFrame")

print("Transforming raw data")

# Transform the DataFrame to have one row per observation, with columns series_id, date, and value (null for missing observations)
df = (
    df.select("series_id", F.explode_outer(F.col("observations")).alias("obs"))
      .select(
            "series_id",
            F.col("obs.date").cast("date").alias("date"),
            F.col("obs.value").cast("double").alias("value")
      )
)

# ── Validate ──────────────────────────────────────────────────────────────────────

print("Evaluating Glue Data Quality rules")

dq_results = EvaluateDataQuality.apply(
    frame=df,
    ruleset='''
        Rules = [
            IsPrimaryKey "series_id" "date",
            ColumnValues "series_id" in ["DGS10","DGS2","DGS30","DGS3MO","DCOILWTICO","VIXCLS","DTWEXBGS","ICSA","MORTGAGE30US","SP500","IHLIDXUSTPSOFTDEVE","USREC","WALCL","PAYEMS","CPIAUCSL","CIVPART","UNRATE","UMCSENT","FEDFUNDS","M2SL","PSAVERT","SAHMREALTIME","CSUSHPINSA","GDPC1","A191RL1Q225SBEA","JTSJOL","M2V","MSPUS","GFDEGDQ188S","MEHOINUSA672N"]
        ]
    ''',
    publishing_options={
        "dataQualityEvaluationContext": "FREDEvaluateDataQuality",
        "enableDataQualityCloudWatchMetrics": "true",
        "enableDataQualityResultsPublishing": "true"
    }
)

# Raise an exception if any Glue Data Quality rule failed
failed_rules = dq_results.filter(dq_results["Outcome"] == "Failed")
failed_rules_count = failed_rules.count()

if failed_rules_count > 0:
    print(f"Glue Data Quality evaluation failed: {failed_rules_count} rule(s) did not pass")
    failed_rules.show(truncate=False)
    raise Exception(f"Glue Data Quality evaluation failed: {failed_rules_count} rule(s) did not pass")

print("Glue Data Quality evaluation passed: all rules passed")

# ── Write Parquet ──────────────────────────────────────────────────────────────────────

# Get bucket name from raw_path for use in processed path
bucket_name = urlparse(raw_path).netloc
processed_path = f"s3://{bucket_name}/processed/economic_indicators/"

print(f"Writing partitioned Parquet to {processed_path}")

df.write.mode("overwrite").partitionBy("series_id").parquet(processed_path)

print("Glue ETL job complete")