from pyspark.sql import SparkSession
from file_lib import helper

spark = SparkSession.builder.getOrCreate()
df = spark.range(1000).toDF("region", "revenue", "a", "b", "c")

# ── Cross-file call (NOT caught — helper cost is unknown to this file) ────────
# helper has 5 shuffles per call; 2 calls = 10 shuffles → should fire,
# but PERF003 won't catch this because helper is defined in file_lib.py
df = helper(df)
df = helper(df)
df = helper(df)
df = helper(df)
df = helper(df)
df = helper(df)