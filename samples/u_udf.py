"""
UDF rules (U001–U007) — all edge cases.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, IntegerType, BooleanType, ArrayType

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# U003 — UDF in general (performance penalty vs native Spark functions)
@udf(returnType=StringType())
def generic_udf(x):
    return str(x)

df_u3 = df.withColumn("out", generic_udf(col("any_col")))

# U001 — UDF applied to a string-typed column
@udf(returnType=StringType())
def upper_udf(s: str) -> str:
    return s.upper() if s else None

df_u1 = df.withColumn("name_upper", upper_udf(col("name")))

# U002 — UDF applied to an array-typed column
@udf(returnType=ArrayType(StringType()))
def dedup_array_udf(arr):
    return list(set(arr)) if arr else []

df_u2 = df.withColumn("arr_deduped", dedup_array_udf(col("tags")))

# U004 — UDF calling another UDF (nested UDF)
@udf(returnType=StringType())
def inner_udf(x: str) -> str:
    return x.strip()

@udf(returnType=StringType())
def outer_udf(x: str) -> str:
    return inner_udf(x)  # U004 — nested UDF call

df_u4 = df.withColumn("cleaned", outer_udf(col("raw")))

# U005 — loop inside UDF body (prefer transform / aggregate)
@udf(returnType=IntegerType())
def sum_loop_udf(values):
    total = 0
    for v in values:  # U005 — loop inside UDF
        total += v
    return total

df_u5 = df.withColumn("total", sum_loop_udf(col("numbers")))

# U006 — all() inside UDF (prefer forall() Spark function)
@udf(returnType=BooleanType())
def all_positive_udf(values):
    return all(v > 0 for v in values)  # U006 — all() inside UDF

df_u6 = df.withColumn("all_pos", all_positive_udf(col("numbers")))

# U007 — any() inside UDF (prefer exists() Spark function)
@udf(returnType=BooleanType())
def any_negative_udf(values):
    return any(v < 0 for v in values)  # U007 — any() inside UDF

df_u7 = df.withColumn("has_neg", any_negative_udf(col("numbers")))
