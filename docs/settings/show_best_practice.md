# show_best_practice

**Type:** `bool`
**Default:** `false`

---

## Description

When `true`, each violation in the terminal output is followed by **best-practice guidance** showing the recommended alternative.

The text is extracted from the `## Best practices` section of each rule's documentation page. It provides concrete code patterns to replace the flagged antipattern.

---

## Example

```toml
[tool.pyspark-antipattern]
show_best_practice = true
```

Terminal output with `show_best_practice = true`:

```
error[D001]: Avoid using collect()
  --> pipeline.py:42:10
   |
42 |     result = df.collect()
   |              ^^^^^^^^^^^^
   |
   best practice: Use distributed operations like .select(), .filter(),
                  .groupBy() instead of bringing data to the driver.
                  Write results to storage with .write.parquet() instead.
```

---

## Notes

!!! tip
    Combine with `show_information = true` for the most educational output — useful when onboarding engineers to PySpark best practices.
