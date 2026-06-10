from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("youtube_analysis") \
    .getOrCreate()

df = spark.read.csv("data/USvideos.csv", header=True, inferSchema=True)

df = df \
    .withColumn("views", col("views").try_cast(IntegerType())) \
    .withColumn("likes", col("likes").try_cast(IntegerType())) \
    .withColumn("dislikes", col("dislikes").try_cast(IntegerType())) \
    .withColumn("comment_count", col("comment_count").try_cast(IntegerType()))
    
df = df.withColumn(
    "engagement_rate",
    (col("likes") + col("comment_count")) / col("views") * 100
)

df.groupBy("category_id") \
    .agg(
        count("video_id").alias("total_videos"),
        avg("engagement_rate").alias("avg_engagement")
    ) \
    .orderBy(desc("avg_engagement")) \
    .show()
    
df.toPandas().to_csv("output/youtube_transformed.csv", index=False)