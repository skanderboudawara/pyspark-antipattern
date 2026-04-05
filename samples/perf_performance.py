"""
Performance rules (PERF001–PERF008) — all edge cases.
"""
from pyspark.sql import SparkSession
from pyspark.sql.storagelevel import StorageLevel
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# PERF001 — rdd.collect() instead of toPandas()
rdd_result = df.rdd.collect()

# PERF002 — multiple getOrCreate() calls (use getActiveSession() for subsequent references)
spark_a = SparkSession.builder.getOrCreate()
spark_b = SparkSession.builder.getOrCreate()  # PERF002 — redundant second getOrCreate

# PERF003 — too many shuffle operations without a checkpoint (default threshold = 9)
# Each of the following counts as a shuffle stage:
df_p3 = df
df_p3 = df_p3.groupBy("a").agg(F.sum("b"))       # shuffle 1
df_p3 = df_p3.distinct()                          # shuffle 2
df_p3 = df_p3.sort("a")                           # shuffle 3
df_p3 = df_p3.repartition(200)                    # shuffle 4
df_p3 = df_p3.sortWithinPartitions("b")           # shuffle 5
df_p3 = df_p3.dropDuplicates(["a"])               # shuffle 6
df_p3 = df_p3.orderBy("b")                        # shuffle 7
df_p3 = df_p3.groupBy("b").agg(F.count("a"))      # shuffle 8
df_p3 = df_p3.distinct()                          # shuffle 9
df_p3 = df_p3.sort("b")                           # shuffle 10 — PERF003 fires

# PERF004 — persist() without an explicit StorageLevel
df.persist()
df.cache()  # also PERF004 — no explicit level

# PERF005 — persist() called but unpersist() never called (resource leak)
df_p5 = spark.read.parquet("data/other/")
df_p5.persist(StorageLevel.MEMORY_AND_DISK)
result = df_p5.filter(F.col("active") == True).collect()
# no df_p5.unpersist() — PERF005

# PERF006 — checkpoint() / localCheckpoint() without explicit eager argument
df_p6 = spark.read.parquet("data/")
df_p6 = df_p6.checkpoint()             # PERF006 — eager argument missing
df_p6 = df_p6.localCheckpoint()        # PERF006 — eager argument missing

# Correct form for reference:
# df_p6 = df_p6.checkpoint(eager=True)
# df_p6 = df_p6.localCheckpoint(eager=False)

# PERF007 — DataFrame used 2+ times without caching (lineage recomputed)
df_base = spark.read.parquet("data/base/")  # no persist/cache
df_other = spark.read.parquet("data/other/")
df_joined = df_base.join(df_other, "id")    # use 1 of df_base
df_unioned = df_base.union(df_other)        # use 2 of df_base — PERF007 fires

# PERF008 — spark.read.csv() wrapping sc.parallelize() (anti-pattern)
sc = spark.sparkContext
raw_data = [("Alice", 30), ("Bob", 25)]
df_p8 = spark.read.csv(sc.parallelize(raw_data))  # PERF008
