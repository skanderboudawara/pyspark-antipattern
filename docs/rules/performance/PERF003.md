# PERF003 — Too many shuffle operations without a checkpoint

**Category:** Performance
**Default severity:** Error

---

## Severity

🔴 **HIGH** — Major performance impact.

## Information

PySpark shuffle operations (joins, groupBy, sort, repartition, distinct, etc.) are expensive: they involve serialisation, network transfer, and disk I/O across all executor nodes. When many shuffles accumulate in a single lineage without a checkpoint, Spark must re-execute the entire chain every time an action is triggered. This makes the DAG fragile, slow, and hard to debug.

`PERF003` fires when more than `max_shuffle_operations` shuffle-inducing calls occur between two `checkpoint()` / `localCheckpoint()` calls (or between the start of a scope and the first checkpoint). The counter also tracks **function call costs**: if a helper function internally performs N shuffles, every call to that function adds N to the running total in the caller.

Shuffle operations tracked:

`groupBy`, `agg`, `join`, `repartition`, `distinct`, `dropDuplicates`, `orderBy`, `sort`, `sortWithinPartitions`, `reduceByKey`, `groupByKey`, `aggregateByKey`, `combineByKey`, `cogroup`, `cartesian`, `intersection`, `subtractByKey`, `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin`

---

## Best practices

- Call `.localCheckpoint()` (or `.checkpoint()`) after a heavy shuffle stage to materialise the result and truncate the lineage.
- Prefer `.localCheckpoint()` for intermediate checkpoints — it is faster because it writes to executor local storage, not HDFS.
- Use `.checkpoint()` when the result must survive executor failures (e.g. long iterative algorithms).
- Group related shuffle operations together in helper functions and checkpoint the result before passing it downstream.
- Tune `max_shuffle_operations` in `pyproject.toml` to match your cluster's memory and DAG complexity tolerance.

```python
# Bad — 10 shuffles, no checkpoint
df = (
    df
    .join(dim, "id")          # 1
    .groupBy("region")        # 2
    .agg(F.sum("revenue"))    # 3
    .distinct()               # 4
    .sort("revenue")          # 5
    .join(meta, "region")     # 6
    .repartition(200)         # 7
    .dropDuplicates(["id"])   # 8
    .orderBy("id")            # 9
    .agg(F.count("*"))        # 10  ← PERF003 fires here
)

# Good — checkpoint after the expensive join/group stage
df = (
    df
    .join(dim, "id")
    .groupBy("region")
    .agg(F.sum("revenue"))
    .distinct()
    .localCheckpoint()        # ← truncate lineage
)
df = (
    df
    .sort("revenue")
    .join(meta, "region")
    .repartition(200)
    .dropDuplicates(["id"])
    .orderBy("id")
    .agg(F.count("*"))
)
```
