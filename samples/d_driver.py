"""
Driver rules (D001–D007) — all edge cases.
Actions that pull data to the driver node.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# D001 — collect() pulls entire dataset to driver
result = df.collect()
rows = df.filter(col("active") == True).collect()

# D003 — show() / display() triggers a driver action
df.show()
df.show(10)
df.filter(col("a") > 0).show(truncate=False)

# D004 — count() triggers a full scan
n = df.count()
n_filtered = df.filter(col("active") == True).count()

# D005 — rdd.isEmpty() instead of DataFrame.isEmpty()
is_empty_rdd = df.rdd.isEmpty()

# D006 — df.count() == 0 pattern instead of isEmpty()
if df.count() == 0:
    pass

if df.filter(col("active") == True).count() == 0:
    pass

# D007 — df.filter(...).count() == 0 instead of df.filter(...).isEmpty()
if df.filter(col("value") > 100).count() == 0:
    print("no results")

if df.filter(col("status") == "active").count() == 0:
    raise ValueError("empty result set")
