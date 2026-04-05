"""
Array rules (ARR001–ARR006) — all edge cases.
"""
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# ARR001 — array_distinct(collect_list()) — use collect_set() instead
df_arr1 = df.groupBy("id").agg(
    F.array_distinct(F.collect_list("tag")).alias("unique_tags")
)

# ARR002 — array_except(col, None) — use array_compact() instead
df_arr2 = df.withColumn(
    "non_null_items",
    F.array_except(F.col("items"), F.lit(None)),
)

# ARR003 — array_distinct(collect_set()) — redundant, collect_set is already distinct
df_arr3 = df.groupBy("id").agg(
    F.array_distinct(F.collect_set("tag")).alias("tags")
)

# ARR004 — size(collect_set()) in agg — use count_distinct() instead
df_arr4 = df.groupBy("category").agg(
    F.size(F.collect_set("user_id")).alias("distinct_user_count")
)

# ARR005 — size(collect_list()) in agg — use count() instead
df_arr5 = df.groupBy("category").agg(
    F.size(F.collect_list("user_id")).alias("user_count")
)

# ARR006 — size(collect_list().over(window)) — use count().over(window) instead
w = Window.partitionBy("category")
df_arr6 = df.withColumn(
    "items_in_category",
    F.size(F.collect_list("item_id").over(w)),
)

# Edge case: ARR006 with ordered window
w_ordered = Window.partitionBy("category").orderBy("event_date")
df_arr6b = df.withColumn(
    "running_count",
    F.size(F.collect_list("item_id").over(w_ordered)),
)
