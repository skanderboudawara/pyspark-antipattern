from pyspark.sql import SparkSession
from file_lib import medium

spark = SparkSession.builder.getOrCreate()
df = spark.range(1000).toDF("region", "revenue", "a", "b", "c")

# ── Cross-file call (NOT caught — medium cost is unknown to this file) ────────
# medium has 5 shuffles per call; 2 calls = 10 shuffles → should fire,
df = medium(df)
df = medium(df)
df = medium(df)
df = medium(df)
df = medium(df)
df = medium(df)