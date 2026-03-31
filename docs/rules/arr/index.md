# ARR — Array

Rules that catch inefficient or incorrect use of PySpark array functions.

| Rule | Title |
|---|---|
| [ARR001](ARR001.md) | Avoid `array_distinct(collect_list())` — use `collect_set()` instead |
| [ARR002](ARR002.md) | Avoid `array_except(col, None/lit(None))` — use `array_compact()` instead |
