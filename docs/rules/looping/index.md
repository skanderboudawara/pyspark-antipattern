# L — Looping

Rules that detect DataFrame operations inside Python loops. Iterating over a DataFrame or calling DataFrame transformations in a loop prevents Spark from building an optimal query plan and causes plan bloat.

| Rule | Title |
|---|---|
| [L001](L001.md) | Avoid looping without `.localCheckpoint()` or `.checkpoint()` |
| [L002](L002.md) | Avoid while loops with DataFrames |
| [L003](L003.md) | Avoid calling `withColumn()` inside a loop |
