"""
Format rules (F001–F020) — all edge cases.
"""
from datetime import datetime, date, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, lit

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# F001 — withColumn chained with withColumnRenamed
df_f1 = df.withColumn("a", col("x") + 1).withColumnRenamed("a", "alpha")

# F002 — drop instead of explicit select
df_f2 = df.drop("unwanted_col")

# F003 — selectExpr instead of select with column expressions
df_f3 = df.selectExpr("a + 1 as a_inc", "upper(b) as b_upper")

# F004 — spark.sql instead of native DataFrame API
df_f4 = spark.sql("SELECT id, name FROM my_table WHERE active = true")

# F005 — stacking multiple withColumn calls (prefer withColumns)
df_f5 = (
    df.withColumn("a", col("x") + 1)
    .withColumn("b", col("y") + 2)
    .withColumn("c", col("z") * 3)
)

# F006 — stacking multiple withColumnRenamed calls (prefer withColumnsRenamed)
df_f6 = df.withColumnRenamed("col_a", "a").withColumnRenamed("col_b", "b").withColumnRenamed("col_c", "c")

# F007 — select before filter (clarity: filter first, then select)
df_f7 = df.select("id", "name", "value").filter(col("value") > 100)

# F008 — print instead of logging
print("Starting pipeline")
print(f"Row count: {df.count()}")

# F009 — nested when calls (prefer chained .when().when().otherwise())
df_f9 = df.withColumn(
    "category",
    when(col("score") > 90, "A")
    .when(col("score") > 70, when(col("bonus") > 0, "B+").otherwise("B"))
    .otherwise("C"),
)

# F010 — when without otherwise
df_f10 = df.withColumn("flag", when(col("active") == True, lit("yes")))

# F011 — backslash line continuation (prefer parentheses)
df_f11 = df \
    .filter(col("a") > 0) \
    .select("a", "b")

# F012 — literal value not wrapped with lit()
df_f12 = df.withColumn("constant", col("a") + 1)
df_f12b = df.filter(col("status") == "active")

# F013 — column name with dunder prefix/suffix (pandas-on-Spark reserved)
df_f13 = df.withColumn("__index__", col("id"))
df_f13b = df.withColumn("__metadata__", col("info"))

# F014 — explode_outer (prefer explicit null handling with higher-order functions)
df_f14 = df.withColumn("items", F.explode_outer("array_col"))

# F015 — multiple consecutive filter calls (combine with & / |)
df_f15 = df.filter(col("a") > 0).filter(col("b") < 100).filter(col("c").isNotNull())

# F016 — long chain of intermediate DataFrame variable names (>2 renames)
df_step1 = df.filter(col("active") == True)
df_step2 = df_step1.withColumn("score_adj", col("score") * 1.1)
df_step3 = df_step2.select("id", "score_adj")
df_step4 = df_step3.withColumnRenamed("score_adj", "final_score")

# F017 — expr() with SQL string (prefer native PySpark functions)
df_f17 = df.withColumn("result", F.expr("a + b * 2"))
df_f17b = df.select(F.expr("year(event_date) as event_year"))

# F018 — Python datetime/date/timedelta objects in Spark expressions
df_f18a = df.filter(col("created_at") > datetime(2023, 1, 1))
df_f18b = df.filter(col("event_date") == date(2023, 6, 15))
df_f18c = df.filter(col("duration") < timedelta(days=7))

# F019 — inferSchema or mergeSchema (define schema explicitly instead)
df_f19a = spark.read.option("inferSchema", "true").csv("data.csv")
df_f19b = spark.read.option("mergeSchema", "true").parquet("data/")

# F020 — select("*") (prefer explicit column names)
df_f20 = df.select("*")
df_f20b = df.filter(col("a") > 0).select("*")
