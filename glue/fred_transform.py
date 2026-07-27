import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from awsgluedq.transforms import EvaluateDataQuality
from urllib.parse import urlparse

args = getResolvedOptions(sys.argv, ["raw_path"])
raw_path = args["raw_path"]

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df = spark.read.json(raw_path)

df = (
    df.select("series_id", F.explode_outer(F.col("observations")).alias("obs"))
      .select(
            "series_id",
            F.col("obs.date").cast("date").alias("date"),
            F.col("obs.value").cast("double").alias("value")
      )
)

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
dq_results.show(truncate=False)

failed_rules = dq_results.filter(dq_results["Outcome"] == "Failed")
failed_rules_count = failed_rules.count()
if failed_rules_count > 0:
    failed_rules.show(truncate=False)
    raise Exception(f"Data quality check failed: {failed_rules_count} rule(s) did not pass")

bucket_name = urlparse(raw_path).netloc
processed_path = f"s3://{bucket_name}/processed/economic_indicators/"
df.write.mode("overwrite").partitionBy("series_id").parquet(processed_path)