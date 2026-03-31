# Rules

All rules are organized by category. Each rule page explains what is flagged, why it matters, and how to fix it.

---

## ARR — Array

| Rule | Title |
|---|---|
| [ARR001](arr/ARR001.md) | Avoid `array_distinct(collect_list())` — use `collect_set()` instead |
| [ARR002](arr/ARR002.md) | Avoid `array_except(col, None/lit(None))` — use `array_compact()` instead |

---

## D — Driver

| Rule | Title |
|---|---|
| [D001](driver/D001.md) | Avoid using `collect()` |
| [D002](driver/D002.md) | Avoid accessing `.rdd` |
| [D003](driver/D003.md) | Avoid `.show()` in production |
| [D004](driver/D004.md) | Avoid `.count()` on large DataFrames |
| [D005](driver/D005.md) | Avoid `.rdd.isEmpty()` — use `.isEmpty()` directly |
| [D006](driver/D006.md) | Avoid `df.count() == 0` — use `.isEmpty()` |
| [D007](driver/D007.md) | Avoid `.filter(...).count() == 0` — use `.filter(...).isEmpty()` |
| [D008](driver/D008.md) | Avoid `.display()` in production |

---

## F — Format

| Rule | Title |
|---|---|
| [F001](format/F001.md) | Avoid chaining `withColumn()` and `withColumnRenamed()` |
| [F002](format/F002.md) | Avoid `drop()` — use `select()` for explicit columns |
| [F003](format/F003.md) | Avoid `selectExpr()` — prefer `select()` with `col()` |
| [F004](format/F004.md) | Avoid `spark.sql()` — prefer native DataFrame API |
| [F005](format/F005.md) | Avoid stacking multiple `withColumn()` — use `withColumns()` |
| [F006](format/F006.md) | Avoid stacking multiple `withColumnRenamed()` — use `withColumnsRenamed()` |
| [F007](format/F007.md) | Prefer `filter()` before `select()` for clarity |
| [F008](format/F008.md) | Avoid `print()` — prefer the logging module |
| [F009](format/F009.md) | Avoid nested `when()` — use stacked `.when().when().otherwise()` |
| [F010](format/F010.md) | Always include `otherwise()` at the end of a `when()` chain |
| [F011](format/F011.md) | Avoid backslash line continuation — use parentheses |
| [F012](format/F012.md) | Always wrap literal values with `lit()` |
| [F013](format/F013.md) | Avoid reserved column names with `__` prefix and `__` suffix |
| [F014](format/F014.md) | Avoid `explode_outer()` — handle nulls with higher-order functions |
| [F015](format/F015.md) | Avoid multiple consecutive `filter()` calls — combine conditions |
| [F016](format/F016.md) | Avoid long DataFrame renaming chains — overwrite the same variable |
| [F017](format/F017.md) | Avoid `expr()` — use native PySpark functions instead |
| [F018](format/F018.md) | Use Spark native datetime functions instead of Python datetime objects |

---

## L — Looping

| Rule | Title |
|---|---|
| [L001](looping/L001.md) | Avoid looping without `.localCheckpoint()` or `.checkpoint()` |
| [L002](looping/L002.md) | Avoid while loops with DataFrames |
| [L003](looping/L003.md) | Avoid calling `withColumn()` inside a loop |

---

## P — Pandas

| Rule | Title |
|---|---|
| [P001](pandas/P001.md) | `.toPandas()` without enabling Arrow optimization |

---

## PERF — Performance

| Rule | Title |
|---|---|
| [PERF001](performance/PERF001.md) | Avoid `.rdd.collect()` — use `.toPandas()` for driver-side consumption |
| [PERF002](performance/PERF002.md) | Too many `getOrCreate()` calls — use `getActiveSession()` everywhere else |
| [PERF003](performance/PERF003.md) | Too many shuffle operations without a checkpoint |

---

## S — Shuffle

| Rule | Title |
|---|---|
| [S001](shuffle/S001.md) | Missing `.coalesce()` after `.union()` / `.unionByName()` |
| [S002](shuffle/S002.md) | Join without a broadcast or merge hint |
| [S003](shuffle/S003.md) | `.groupBy()` directly followed by `.distinct()` |
| [S004](shuffle/S004.md) | Too many `.distinct()` operations in one file |
| [S005](shuffle/S005.md) | `.repartition()` with fewer partitions than the Spark default |
| [S006](shuffle/S006.md) | `.repartition()` with more partitions than the Spark default |
| [S007](shuffle/S007.md) | Avoid `repartition(1)` or `coalesce(1)` |
| [S008](shuffle/S008.md) | Overusing `explode()` / `explode_outer()` |
| [S009](shuffle/S009.md) | Prefer `mapPartitions()` over `map()` for row-level transforms |
| [S010](shuffle/S010.md) | Avoid `crossJoin()` — produces a Cartesian product |
| [S011](shuffle/S011.md) | Join without join conditions causes a nested-loop scan |
| [S012](shuffle/S012.md) | Avoid inner join followed by filter — prefer `leftSemi` join |
| [S013](shuffle/S013.md) | Avoid `reduceByKey()` — use DataFrame `groupBy().agg()` instead |

---

## U — UDF

| Rule | Title |
|---|---|
| [U001](udf/U001.md) | Avoid UDFs that return `StringType` — use built-in string functions |
| [U002](udf/U002.md) | Avoid UDFs that return `ArrayType` — use built-in array functions |
| [U003](udf/U003.md) | Avoid UDFs — use Spark built-in functions instead |
| [U004](udf/U004.md) | Avoid nested UDF calls — merge logic or use plain Python helpers |
