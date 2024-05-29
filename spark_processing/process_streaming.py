from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AdvertiseX Data Processing") \
    .getOrCreate()

# Define schema for ad impressions
ad_impressions_schema = StructType([
    StructField("ad_creative_id", StringType()),
    StructField("user_id", StringType()),
    StructField("timestamp", LongType()),
    StructField("website", StringType())
])

# Read data from Kafka
ad_impressions_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ad_impressions") \
    .load()

# Parse JSON data
ad_impressions_parsed = ad_impressions_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), ad_impressions_schema).alias("data")) \
    .select("data.*")

# Process data (e.g., deduplication)
deduplicated_ad_impressions = ad_impressions_parsed.dropDuplicates(["user_id", "timestamp"])

# Write processed data to S3
def write_to_s3(df, epoch_id):
    df.write \
        .format("parquet") \
        .mode("append") \
        .save("s3a://your-bucket/ad_impressions")

deduplicated_ad_impressions.writeStream \
    .foreachBatch(write_to_s3) \
    .start() \
    .awaitTermination()
