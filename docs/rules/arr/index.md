# ARR — Array

Rules that catch inefficient or incorrect use of PySpark array functions.

| Rule | Title |
|---|---|
| [ARR001](ARR001.md) | Avoid `array_distinct(collect_list())` — use `collect_set()` instead |
| [ARR002](ARR002.md) | Avoid `array_except(col, None/lit(None))` — use `array_compact()` instead |
| [ARR003](ARR003.md) | Avoid `array_distinct(collect_set())` — `collect_set` already returns distinct values |
| [ARR004](ARR004.md) | Avoid `size(collect_set())` inside `.agg()` — use `count_distinct()` instead |
| [ARR005](ARR005.md) | Avoid `size(collect_list())` inside `.agg()` — use `count()` instead |
| [ARR006](ARR006.md) | Avoid `size(collect_list().over(w))` — use `count().over(w)` instead |
