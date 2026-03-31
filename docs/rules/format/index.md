# F — Format

Rules that enforce idiomatic use of the PySpark DataFrame API — covering code style, readability, and correct function usage.

| Rule | Title |
|---|---|
| [F001](F001.md) | Avoid chaining `withColumn()` and `withColumnRenamed()` |
| [F002](F002.md) | Avoid `drop()` — use `select()` for explicit columns |
| [F003](F003.md) | Avoid `selectExpr()` — prefer `select()` with `col()` |
| [F004](F004.md) | Avoid `spark.sql()` — prefer native DataFrame API |
| [F005](F005.md) | Avoid stacking multiple `withColumn()` — use `withColumns()` |
| [F006](F006.md) | Avoid stacking multiple `withColumnRenamed()` — use `withColumnsRenamed()` |
| [F007](F007.md) | Prefer `filter()` before `select()` for clarity |
| [F008](F008.md) | Avoid `print()` — prefer the logging module |
| [F009](F009.md) | Avoid nested `when()` — use stacked `.when().when().otherwise()` |
| [F010](F010.md) | Always include `otherwise()` at the end of a `when()` chain |
| [F011](F011.md) | Avoid backslash line continuation — use parentheses |
| [F012](F012.md) | Always wrap literal values with `lit()` |
| [F013](F013.md) | Avoid reserved column names with `__` prefix and `__` suffix |
| [F014](F014.md) | Avoid `explode_outer()` — handle nulls with higher-order functions |
| [F015](F015.md) | Avoid multiple consecutive `filter()` calls — combine conditions |
| [F016](F016.md) | Avoid long DataFrame renaming chains — overwrite the same variable |
| [F017](F017.md) | Avoid `expr()` — use native PySpark functions instead |
| [F018](F018.md) | Use Spark native datetime functions instead of Python datetime objects |
