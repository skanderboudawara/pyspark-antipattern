# severity

**Type:** `str`
**Default:** `null` (all violations reported)
**Accepted values:** `"low"`, `"medium"`, `"high"`

---

## Description

Filters violations by their static **performance impact** level. Only violations whose impact meets or exceeds the configured value are reported. Violations below the threshold produce no output and do not affect the exit code.

Each rule carries one of three impact levels:

| Level | Badge | Meaning |
|---|---|---|
| `"low"` | 🟢 **LOW** | Minor impact — style, API correctness, small inefficiencies |
| `"medium"` | 🟡 **MEDIUM** | Moderate impact — avoidable overhead at scale |
| `"high"` | 🔴 **HIGH** | Major impact — OOM risk, full scans, shuffle explosion |

The impact level is displayed as a colored badge in the terminal output next to the rule ID:

```
error[D001][HIGH]: Avoid using collect()
error[F005][LOW]: Avoid stacking multiple withColumn() calls
```

---

## Example

```toml
[tool.pyspark-antipattern]
# Report only MEDIUM and HIGH violations
severity = "medium"
```

```toml
[tool.pyspark-antipattern]
# Report only HIGH violations — useful for blocking CI on critical issues only
severity = "high"
```

---

## CLI equivalent

```bash
pyspark-antipattern check src/ --severity=medium
pyspark-antipattern check src/ --severity=high
```

When both `pyproject.toml` and `--severity` are set, the CLI flag takes precedence.

---

## Notes

- Setting `severity = "low"` is equivalent to the default (all violations reported).
- `severity` filters by the rule's static impact level, which is independent of the `warn` / `ignore` / `select` options. A rule silenced via `ignore` is never reported regardless of severity.
- To see the impact level of every rule, browse the [rules reference](../rules/index.md) — each rule page has a `## Severity` section.
