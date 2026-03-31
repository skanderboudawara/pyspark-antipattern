# U — UDF

Rules that flag user-defined functions where native PySpark equivalents exist. UDFs are black boxes to the Spark optimizer and typically 10–100× slower than built-in functions.

| Rule | Title |
|---|---|
| [U001](U001.md) | Avoid UDFs that return `StringType` — use built-in string functions |
| [U002](U002.md) | Avoid UDFs that return `ArrayType` — use built-in array functions |
| [U003](U003.md) | Avoid UDFs — use Spark built-in functions instead |
