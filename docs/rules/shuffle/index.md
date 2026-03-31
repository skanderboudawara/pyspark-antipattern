# S — Shuffle

Rules that flag patterns that cause expensive data movement across the cluster — wide transformations, unoptimized joins, and over-partitioning.

| Rule | Title |
|---|---|
| [S001](S001.md) | Missing `.coalesce()` after `.union()` / `.unionByName()` |
| [S002](S002.md) | Join without a broadcast or merge hint |
| [S003](S003.md) | `.groupBy()` directly followed by `.distinct()` |
| [S004](S004.md) | Too many `.distinct()` operations in one file |
| [S005](S005.md) | `.repartition()` with fewer partitions than the Spark default |
| [S006](S006.md) | `.repartition()` with more partitions than the Spark default |
| [S007](S007.md) | Avoid `repartition(1)` or `coalesce(1)` |
| [S008](S008.md) | Overusing `explode()` / `explode_outer()` |
| [S009](S009.md) | Prefer `mapPartitions()` over `map()` for row-level transforms |
| [S010](S010.md) | Avoid `crossJoin()` — produces a Cartesian product |
| [S011](S011.md) | Join without join conditions causes a nested-loop scan |
| [S012](S012.md) | Avoid inner join followed by filter — prefer `leftSemi` join |
| [S013](S013.md) | Avoid `reduceByKey()` — use DataFrame `groupBy().agg()` instead |
