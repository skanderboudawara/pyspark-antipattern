"""
Shuffle rules (S001–S014) — all edge cases.
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.parquet("data/left/")
df2 = spark.read.parquet("data/right/")

# S001 — union / unionByName without coalesce or repartition after
df_union = df1.union(df2)
df_union_by_name = df1.unionByName(df2)

# S002 — join without a broadcast/merge hint
df_join = df1.join(df2, "id")
df_join_cond = df1.join(df2, df1.id == df2.id, "left")

# S003 — groupBy followed immediately by distinct / dropDuplicates (redundant shuffle)
df_grp_distinct = df1.groupBy("a").agg(F.sum("b")).distinct()
df_grp_dedup = df1.groupBy("a").agg(F.count("b")).dropDuplicates()

# S004 — too many distinct calls (default threshold = 5)
df_s4 = df1
df_s4 = df_s4.distinct()
df_s4 = df_s4.distinct()
df_s4 = df_s4.distinct()
df_s4 = df_s4.distinct()
df_s4 = df_s4.distinct()
df_s4 = df_s4.distinct()  # 6th distinct — S004 fires

# S005 — repartition with fewer partitions than Spark default (200)
df_few = df1.repartition(10)

# S006 — repartition with more partitions than Spark default (200)
df_many = df1.repartition(800)

# S007 — repartition(1) or coalesce(1) forces single partition
df_one_rep = df1.repartition(1)
df_one_coal = df1.coalesce(1)

# S008 — too many explode / explode_outer calls (default threshold = 3)
df_s8 = df1
df_s8 = df_s8.withColumn("e1", F.explode("arr1"))
df_s8 = df_s8.withColumn("e2", F.explode("arr2"))
df_s8 = df_s8.withColumn("e3", F.explode("arr3"))
df_s8 = df_s8.withColumn("e4", F.explode_outer("arr4"))  # 4th — S008 fires

# S009 — map on RDD instead of mapPartitions
rdd = df1.rdd
rdd_mapped = rdd.map(lambda row: row)

# S010 — crossJoin (Cartesian product)
df_cross = df1.crossJoin(df2)

# S011 — join with no equality condition (nested loop join)
df_loop_join = df1.join(df2, df1.a > df2.b)

# S012 — inner join immediately followed by filter (prefer leftSemi)
df_join_filter = df1.join(df2, "id").filter(col("a") > 5)

# S013 — reduceByKey on RDD instead of groupBy().agg()
rdd_kv = df1.rdd.map(lambda row: (row["a"], row["b"]))
rdd_reduced = rdd_kv.reduceByKey(lambda a, b: a + b)

# S014 — distinct / dropDuplicates before groupBy (redundant shuffle)
df_dedup_grp = df1.distinct().groupBy("a").agg(F.sum("b"))
df_dd_grp = df1.dropDuplicates(["a"]).groupBy("a").count()
