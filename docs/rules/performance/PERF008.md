# Rule PERF008
Avoid `spark.read.csv(parallelize())` — use `spark.createDataFrame(pd.read_csv())` instead

## Severity

🟡 **MEDIUM** — Moderate performance impact.

## PySpark version

Compatible with **PySpark 2.0** and later.

## Information
Using `sparkContext.parallelize()` to convert an in-memory Python object into an RDD, only
to immediately pass it to `spark.read.csv()`, sends data through the full Spark serialisation
pipeline for no reason:

1. `sparkContext.parallelize(data.split("\n"))` serialises Python objects into JVM-side RDD partitions
2. `spark.read.csv(rdd, ...)` then deserialises and re-parses those partitions as CSV

The data never needed to leave the driver. `spark.createDataFrame(pd.read_csv(...))` parses
the CSV entirely in the driver process via Pandas and hands the result directly to Spark —
no RDD, no JVM round-trip, no unnecessary serialisation overhead.

This applies to both the split form and the inline form:

```python
# split form
rdd = spark.sparkContext.parallelize(data.split("\n"))
df  = spark.read.csv(rdd, header=True, sep=';')

# inline form
df = spark.read.csv(spark.sparkContext.parallelize(data.split("\n")), header=True, sep=';')
```

## Best practices
Parse small, in-memory CSV payloads directly with Pandas and wrap the result in a DataFrame.

### Example

Bad:
```python
from io import StringIO

rdd = spark.sparkContext.parallelize(data.split("\n"))
df  = spark.read.csv(rdd, header=True, sep=';')
```

Good:
```python
import pandas as pd
from io import StringIO

df = spark.createDataFrame(pd.read_csv(StringIO(data), sep=";", dtype="str"))
```
