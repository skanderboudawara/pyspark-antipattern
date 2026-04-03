# Rule PERF001
Avoid `.rdd.collect()` — use `.toPandas()` instead

## Severity

🔴 **HIGH** — Major performance impact.

## PySpark version

Compatible with **PySpark 3.0** and later.

## Information
Calling `.rdd.collect()` to bring data to the driver and then manually converting to Python objects bypasses Spark's optimized data transfer path.

- `.rdd.collect()` forces full deserialization of every row through the RDD layer, which is significantly slower than the columnar transfer used by `.toPandas()`
- Memory usage is higher because the data is materialized as Java/Python objects rather than Arrow buffers
- With Arrow optimization enabled (`spark.sql.execution.arrow.pyspark.enabled = true`), `.toPandas()` uses zero-copy columnar transfer and is orders of magnitude faster
- `.rdd.collect()` returns a list of `Row` objects that still need to be converted manually, adding extra processing overhead

## Best practices
- Use `.toPandas()` for small datasets that need to be brought to the driver
- Enable Arrow optimization for maximum performance: `spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")`
- For large datasets, avoid collecting at all — process in Spark and write results to storage

### Example

Bad:
```python
rows = df.rdd.collect()
data = [(r["id"], r["value"]) for r in rows]
```

Good:
```python
pdf = df.toPandas()
```
