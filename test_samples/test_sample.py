from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/")

# D001 - collect
result = df.collect()

# D003 - show
result.show()

# D004 - count
n = df.count()

# F005 - stacked withColumn
df2 = df.withColumn("a", col("x") + 1).withColumn("b", col("y") + 2).withColumn("c", col("z"))

# F008 - print
print("hello")

# L003 - withColumn inside loop
for i in range(10):
    df = df.withColumn(f"col_{i}", col("x"))

# U003 - UDF
@udf(returnType=StringType())
def my_udf(x):
    return x.upper()

# S010 - crossJoin
df3 = df.crossJoin(df2)

# S007 - coalesce(1)
df4 = df.coalesce(1)

# noqa suppression
bad = df.collect()  # noqa: pap: D001

# S004 - too many distinct
for i in range(10):
    df = df.distinct()
    df = df.localCheckpoint()