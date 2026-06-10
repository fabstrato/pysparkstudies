from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

spark = SparkSession.builder \
    .appName("youtube_analysis") \
    .getOrCreate()
df = spark.read.csv("data/USvideos.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)