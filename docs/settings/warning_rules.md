# warning_rules

**Type:** `list[str]`
**Default:** `[]`

---

## Description

Rules listed here are **downgraded from error to warning**. They are still reported in the output but do not contribute to exit code 1.

Use this for rules your team is aware of but has not yet addressed, or for stylistic rules that are not blockers in your context.

Accepts exact rule IDs or single-letter group prefixes.

---

## Example

```toml
[tool.pyspark-antipattern]
# print() and backslash continuations are reported but don't block CI
warning_rules = ["F008", "F011"]
```

---

## Notes

!!! info
    A rule in `warning_rules` is still visible in the output — it just won't fail the pipeline. Use `ignore_rules` to silence it completely.

- Group prefix `"F"` downgrades all F rules to warnings
- If a rule appears in both `warning_rules` and `ignore_rules`, `ignore_rules` wins
