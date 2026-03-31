# D — Driver

Rules that flag operations which pull data from the distributed cluster to the driver node. These are the most dangerous antipatterns — they can silently OOM the driver or stall a pipeline entirely.

| Rule | Title |
|---|---|
| [D001](D001.md) | Avoid using `collect()` |
| [D002](D002.md) | Avoid accessing `.rdd` |
| [D003](D003.md) | Avoid `.show()` in production |
| [D004](D004.md) | Avoid `.count()` on large DataFrames |
| [D005](D005.md) | Avoid `.rdd.isEmpty()` — use `.isEmpty()` directly |
| [D006](D006.md) | Avoid `df.count() == 0` — use `.isEmpty()` |
| [D007](D007.md) | Avoid `.filter(...).count() == 0` — use `.filter(...).isEmpty()` |
| [D008](D008.md) | Avoid `.display()` in production |
