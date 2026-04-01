df = spark.read.csv("data.csv")
df2 = spark.read.csv("data2.csv")
df2 = df2.cache()
df = df.filter(col("x") > 0).cache()

df3 = df2.join(df, "id")
df3 = df2.union(df)

df = df3.distinct()

df = df.filter(col("y") < 100)

df1 = spark.read.csv("data1.csv")
df1 = df1.join(df, "id")
df1 = df1.union(df)