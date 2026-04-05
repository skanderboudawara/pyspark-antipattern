"""
Pandas rules (P001) — all edge cases.
Arrow optimization should be enabled before calling toPandas().
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# P001 — toPandas() without enabling Arrow optimization
# Arrow is enabled via: spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
pdf = df.toPandas()

# P001 — toPandas() on a filtered/transformed DataFrame (same issue)
pdf_filtered = df.filter(col("active") == True).select("id", "name").toPandas()

# P001 — toPandas() in a function (Arrow still not enabled)
def to_local(dataframe):
    return dataframe.toPandas()  # P001

# Correct pattern for reference (no violation):
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# pdf_fast = df.toPandas()
