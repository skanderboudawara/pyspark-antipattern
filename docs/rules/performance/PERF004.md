# Rule PERF004
Avoid bare `.persist()` — always pass an explicit `StorageLevel`

## Severity

🟡 **MEDIUM** — Moderate performance impact.

## PySpark version

Compatible with **PySpark 3.0** and later.

## Information
Calling `.persist()` with no arguments silently applies the default storage
level (`MEMORY_AND_DISK`). This is an antipattern because:

- The caching strategy is invisible — the next developer (or you in six months)
  has no idea what level was intended or why
- The default may be wrong for your dataset: a large DataFrame that barely fits
  in memory will spill to disk unpredictably; a small lookup table only needs
  `MEMORY_ONLY`
- Tuning becomes a guessing game instead of a deliberate decision visible in
  code review

Always pass a `StorageLevel` explicitly, even if it happens to be the default,
so the intent is documented in the code.

## Best practices
Choose the storage level that matches your use case:

| Level | When to use |
|---|---|
| `MEMORY_ONLY` | DataFrame fits comfortably in memory; fastest |
| `MEMORY_AND_DISK` | Large DataFrame; spills to disk rather than recomputing |
| `MEMORY_AND_DISK_2` | Same + replication for fault tolerance |
| `MEMORY_AND_DISK_DESER` | Skip Java deserialisation overhead; uses more memory |
| `DISK_ONLY` | Very large DataFrame; memory is scarce |
| `DISK_ONLY_2` | Disk + replication |
| `DISK_ONLY_3` | Disk + triple replication |
| `MEMORY_ONLY_2` | Memory + replication |
| `OFF_HEAP` | Store in off-heap memory (Tachyon/Alluxio); reduces GC pressure |
| `NONE` | Explicit no-op (useful to clear a previously set level) |

**Rule of thumb:** Even if `MEMORY_AND_DISK` is the right choice, write it out — it tells the reader the decision was deliberate.

### Example

Bad:
```python
df.persist()
```

Good:
```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.DISK_ONLY)
```
