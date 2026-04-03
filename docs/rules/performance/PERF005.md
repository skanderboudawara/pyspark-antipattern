# Rule PERF005
DataFrame persisted but never unpersisted

## Severity

🟡 **MEDIUM** — Moderate performance impact.

## PySpark version

Compatible with **PySpark 3.0** and later.

## Information
Every `.persist()` call pins the DataFrame's partitions in memory (and/or disk)
for the rest of the Spark session. Forgetting to call `.unpersist()` causes:

- **Memory pressure** that accumulates with every job run, eventually evicting
  other cached data and forcing expensive recomputation
- **Silent leaks** — the cached blocks remain pinned until the session ends or
  the executor is killed, with no warning in logs
- **OOM crashes** in long-running applications or notebooks that persist many
  DataFrames without cleaning up

Every DataFrame that is persisted should have a matching `.unpersist()` call
once the cached data is no longer needed.

## Best practices
- Call `.unpersist()` explicitly once the DataFrame is no longer needed downstream
- If two variables hold the same persisted DataFrame, unpersist **both** names —
  each assignment that received a `.persist()` result must be unpersisted

**Rule of thumb:** Every `.persist()` should have a paired `.unpersist()`.

### Example

Bad:
```python
df  = df.persist()
df2 = df2.persist()

df.unpersist()
# df2 is never unpersisted — memory leak
```

```python
df  = df.persist()
df2 = df.persist()   # df2 holds the same persisted ref

df.unpersist()
# df2.unpersist() was never called — still a leak
```

Good:
```python
df  = df.persist()
df2 = df2.persist()

# ... use df and df2 ...

df.unpersist()
df2.unpersist()
```
