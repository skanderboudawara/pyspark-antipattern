# show_information

**Type:** `bool`
**Default:** `false`

---

## Description

When `true`, each violation in the terminal output is followed by an **inline explanation** of why the pattern is problematic.

The text is extracted from the `## Information` section of each rule's documentation page. It explains the root cause, the risk, and the context in plain language.

---

## Example

```toml
[tool.pyspark-antipattern]
show_information = true
```

Terminal output with `show_information = true`:

```
error[D001]: Avoid using collect()
  --> pipeline.py:42:10
   |
42 |     result = df.collect()
   |              ^^^^^^^^^^^^
   |
   info: Using .collect() pulls all data to the driver node. This defeats
         Spark's distributed nature and can cause OOM errors on large datasets.
```

---

## Notes

!!! tip
    Enable this in local development or code review contexts where the extra context helps. Disable it in CI to keep output compact.
