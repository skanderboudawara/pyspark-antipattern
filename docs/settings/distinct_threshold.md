# distinct_threshold

**Type:** `int`
**Default:** `5`

---

## Description

Controls when rule [S004](../rules/shuffle/S004.md) fires. S004 flags files that contain too many `.distinct()` calls, since each call triggers a full shuffle.

The threshold is a **weighted count** across the whole file — a `.distinct()` inside a loop counts more than one at the top level. When the weighted total exceeds `distinct_threshold`, all `.distinct()` call sites in the file are reported.

---

## Example

```toml
[tool.pyspark-antipattern]
# Allow up to 3 distinct() calls before flagging
distinct_threshold = 3
```

---

## Notes

!!! info
    Setting `distinct_threshold = 0` effectively flags any file that contains even a single `.distinct()` call.

- To silence S004 entirely, add it to `ignore_rules` rather than setting a very high threshold
- Related rule: [S003](../rules/shuffle/S003.md) — flags `.groupBy()` directly followed by `.distinct()`
