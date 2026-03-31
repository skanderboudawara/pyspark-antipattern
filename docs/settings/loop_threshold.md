# loop_threshold

**Type:** `int`
**Default:** `10`

---

## Description

Controls when rules [L001](../rules/looping/L001.md), [L002](../rules/looping/L002.md), and [L003](../rules/looping/L003.md) fire for `for` loops.

- **`for` loops** — only flagged when `range(N)` exceeds `loop_threshold`. A loop over `range(3)` with threshold `10` is not flagged.
- **`while` loops** — always assumed to run 99 iterations and are always flagged regardless of this threshold.

---

## Example

```toml
[tool.pyspark-antipattern]
# Only flag for-loops that could run more than 50 iterations
loop_threshold = 50
```

---

## Notes

!!! info
    `while` loops are always flagged because their iteration count cannot be statically determined — the worst case is assumed.

- Set `loop_threshold = 0` to flag all `for` loops regardless of range
- Related rules: [L001](../rules/looping/L001.md), [L002](../rules/looping/L002.md), [L003](../rules/looping/L003.md)
