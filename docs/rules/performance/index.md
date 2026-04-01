# PERF — Performance

Rules that catch runtime performance antipatterns not covered by other categories.

| Rule | Title |
|---|---|
| [PERF001](PERF001.md) | Avoid `.rdd.collect()` — use `.toPandas()` for driver-side consumption |
| [PERF002](PERF002.md) | Too many `getOrCreate()` calls — use `getActiveSession()` everywhere else |
| [PERF003](PERF003.md) | Too many shuffle operations without a checkpoint |
| [PERF004](PERF004.md) | Avoid bare `.persist()` — always pass an explicit `StorageLevel` |
