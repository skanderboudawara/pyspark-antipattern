# max_shuffle_operations

**Type:** `int`
**Default:** `9`

---

## Description

The maximum number of shuffle-inducing operations allowed between two `checkpoint()` / `localCheckpoint()` calls before rule **PERF003** fires.

When the running counter of shuffle operations exceeds this value without a checkpoint in between, a violation is reported. The counter resets to zero on every checkpoint and also after each violation, so only one violation is emitted per "batch" — not once per extra shuffle.

Shuffle operations counted: `groupBy`, `agg`, `join`, `repartition`, `distinct`, `dropDuplicates`, `orderBy`, `sort`, `sortWithinPartitions`, `reduceByKey`, `groupByKey`, `aggregateByKey`, `combineByKey`, `cogroup`, `cartesian`, `intersection`, `subtractByKey`, `leftOuterJoin`, `rightOuterJoin`, `fullOuterJoin`.

Function call costs are **propagated transitively**: if a helper function internally performs N shuffles (after its last checkpoint), every call to that function contributes N to the caller's running counter.

---

## Example

```toml
[tool.pyspark-antipattern]
# Fire when more than 5 shuffles occur without a checkpoint
max_shuffle_operations = 5
```

---

## Notes

- Lower values are stricter — they force checkpoints more often, reducing DAG size and improving fault-tolerance at the cost of more I/O.
- The default of `9` is a conservative starting point; production pipelines with large datasets may benefit from setting this to `5` or lower.
- Setting this to a very high number (e.g. `999`) effectively disables the rule without adding it to `ignore_rules`.
