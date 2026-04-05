df = spark.read.csv("data.csv")

df = df.filter(col("x") > 0)

df = df.withColumn("y", col("x") * 2)

df = df.distinct()

df1 = spark.read.csv("data1.csv")
df1 = df1.filter(col("z") < 100)
df1 = df1.union(df)