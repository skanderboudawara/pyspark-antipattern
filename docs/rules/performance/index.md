# PERF — Performance

Rules that catch runtime performance antipatterns not covered by other categories.

| Rule | Title |
|---|---|
| [PERF001](PERF001.md) | Avoid `.rdd.collect()` — use `.toPandas()` for driver-side consumption |
| [PERF002](PERF002.md) | Too many `getOrCreate()` calls — use `getActiveSession()` everywhere else |
| [PERF003](PERF003.md) | Too many shuffle operations without a checkpoint |
| [PERF004](PERF004.md) | Avoid bare `.persist()` — always pass an explicit `StorageLevel` |
| [PERF005](PERF005.md) | DataFrame persisted but never unpersisted |
| [PERF006](PERF006.md) | Avoid bare `.checkpoint()` / `.localCheckpoint()` — always pass an explicit `eager` argument |
| [PERF007](PERF007.md) | DataFrame used 2 or more times without caching |
| [PERF008](PERF008.md) | Avoid `spark.read.csv(parallelize())` — use `spark.createDataFrame(pd.read_csv())` instead |
