# Rule PERF007
DataFrame used 2 or more times without caching

!!! warning "Experimental rule"
    This rule is **experimental**. Detection is limited to `.join()`,
    `.union()`, and `.unionByName()` calls — other DataFrame operations
    (e.g. `show`, `collect`, `count`) are intentionally ignored.
    False positives and false negatives are possible; review every
    finding before acting on it.

## Information
Spark uses **lazy evaluation**: every time you trigger an action on a DataFrame,
Spark walks back up the entire lineage graph and re-executes every transformation
from the source.  If the same DataFrame is used as input in two or more places
without being cached, all of that upstream work is repeated for each branch.

```
source ──► filter ──► join  ──► write A
                 └──► agg   ──► write B
```

Without caching `filter`'s result, Spark reads the source and applies the filter
**twice** — once for `write A` and once for `write B`.

Consequences:
- **Doubled (or worse) read and compute costs** — every uncached fork multiplies
  the upstream work
- **Non-determinism with non-idempotent sources** (Kafka, random sampling, etc.)
  — the two re-executions may return different data
- **Longer wall-clock time** in iterative pipelines, ML feature pipelines, and
  any workflow where a cleaned or filtered base DataFrame feeds multiple outputs

Adding `.cache()` (or `.persist(StorageLevel.…)` for explicit memory/disk
control) after the DataFrame is ready materialises it once, and all downstream
consumers read from the cached copy.

## Best practices
Cache the DataFrame immediately after the last transformation that all
downstream consumers share:

```python
# Bad — df is computed twice
df = df.filter(col('country') == 'US')
df2 = df.join(cities, 'city_id')          # full DAG re-executed
df3 = df.groupBy('city').count()          # full DAG re-executed again
```

```python
# Good — df is computed once
df = df.filter(col('country') == 'US')
df = df.cache()                           # materialise once
df2 = df.join(cities, 'city_id')          # reads from cache
df3 = df.groupBy('city').count()          # reads from cache
```

Use `.persist(StorageLevel.MEMORY_AND_DISK)` (or another explicit level) when
the DataFrame may be too large to fit entirely in memory, or when you need the
data to survive executor failures.

```python
from pyspark import StorageLevel

df = df.filter(col('country') == 'US')
df = df.persist(StorageLevel.MEMORY_AND_DISK)
df2 = df.join(cities, 'city_id')
df3 = df.groupBy('city').count()

# ... when done ...
df.unpersist()
```

**Rule of thumb:** If the same DataFrame feeds more than one downstream
computation, cache it.
