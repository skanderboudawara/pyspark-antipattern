rdd = spark.sparkContext.parallelize(data.split("\n"))
df_partial = spark.read.csv(rdd, header=True, sep=';')