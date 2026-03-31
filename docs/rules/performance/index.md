# PERF — Performance

Rules that catch runtime performance antipatterns not covered by other categories.

| Rule | Title |
|---|---|
| [PERF001](PERF001.md) | Avoid `.rdd.collect()` — use `.toPandas()` for driver-side consumption |
| [PERF002](PERF002.md) | Too many `getOrCreate()` calls — use `getActiveSession()` everywhere else |
