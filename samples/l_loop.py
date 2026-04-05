"""
Loop rules (L001–L003) — all edge cases.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# L003 — withColumn called inside a for-loop (always fires regardless of range size)
for i in range(5):
    df = df.withColumn(f"feature_{i}", col("base") * i)

# L001 — for-loop exceeding range threshold (default 10) with no checkpoint inside
# Without localCheckpoint() the lineage grows unboundedly — OOM risk
for i in range(15):  # 15 > default threshold of 10 — L001 fires
    df = df.distinct()

# L001 correct pattern — checkpoint inside the loop breaks the lineage
df2 = spark.read.parquet("data/")
for i in range(15):
    df2 = df2.distinct()
    df2 = df2.localCheckpoint(eager=True)  # breaks lineage — L001 suppressed

# L002 — while-loop with DataFrame operations (assumes unbounded iterations = 99)
condition = True
counter = 0
while condition:  # L002 fires — while-loops always assumed to be long-running
    df = df.filter(col("value") > counter)
    counter += 1
    if counter >= 5:
        condition = False

# L002 + L003 combined — withColumn inside while-loop
df3 = spark.read.parquet("data/")
i = 0
while i < 3:
    df3 = df3.withColumn(f"iter_{i}", col("x") + i)  # both L002 and L003
    i += 1
