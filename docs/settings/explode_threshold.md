# explode_threshold

**Type:** `int`
**Default:** `3`

---

## Description

Controls when rule [S008](../rules/shuffle/S008.md) fires. S008 flags files that overuse `explode()` or `explode_outer()`, since each call can multiply row counts and trigger expensive shuffles downstream.

When the number of `explode` calls in a file exceeds `explode_threshold`, all call sites are reported.

---

## Example

```toml
[tool.pyspark-antipattern]
# Allow up to 5 explode() calls before flagging
explode_threshold = 5
```

---

## Notes

!!! info
    Setting `explode_threshold = 0` flags any file that contains even a single `explode()` call.

- To silence S008 entirely, add it to `ignore_rules`
- Related rule: [F014](../rules/format/F014.md) — flags `explode_outer()` specifically
