# failing_rules

**Type:** `list[str]`
**Default:** all rules

---

## Description

Controls which rules produce **exit code 1** when they fire. By default every rule is a failing rule — any violation blocks the pipeline.

Set this to an explicit list to restrict which rules are hard errors. Rules not listed here will still be reported but will not affect the exit code (they behave as warnings).

Accepts exact rule IDs or single-letter group prefixes.

---

## Example

```toml
[tool.pyspark-antipattern]
# Only D001 and all S rules cause exit code 1 — everything else is informational
failing_rules = ["D001", "S"]
```

---

## Notes

!!! tip
    The strictest setup is the default (all rules fail). Only restrict `failing_rules` when your team has a documented reason to tolerate a pattern in production.

- Group prefix `"D"` matches all rules starting with `D` (D001–D008)
- Works alongside `warning_rules` and `ignore_rules` — ignored rules are always silent regardless of this list
