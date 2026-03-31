# Configuration

Add a `[tool.pyspark-antipattern]` section to your project's `pyproject.toml`.

---

## Full reference

```toml
[tool.pyspark-antipattern]

# Rules listed here cause exit code 1 (default: all rules are failing)
# failing_rules = []

# Downgrade these rules from error to warning (exit code stays 0)
warning_rules = ["F008", "F011"]

# Completely silence these rules — no output, no exit code impact
# Accepts exact rule IDs or single-letter group prefixes
ignore_rules = ["S004"]                # silence one rule
# ignore_rules = ["F"]                 # silence all F rules
# ignore_rules = ["S", "L", "D001"]    # silence all S and L rules, plus D001

# Show inline explanation for each rule that fired (default: false)
show_information = false

# Show best-practice guidance for each rule that fired (default: false)
show_best_practice = false

# S004: flag when the weighted count of .distinct() calls exceeds this (default: 5)
distinct_threshold = 5

# S008: flag when the weighted count of explode() calls exceeds this (default: 3)
explode_threshold = 3

# L001/L002/L003: flag for-loops where range(N) > threshold;
#                 while-loops always assume 99 iterations (default: 10)
loop_threshold = 10
```

---

## Rule severity

Every rule defaults to **error** (exit code 1). You can relax individual rules:

| Option | Effect |
|---|---|
| `warning_rules` | Rule fires but exit code stays 0 |
| `ignore_rules` | Rule is completely silenced |

Both options accept exact IDs (`"D001"`) or single-letter group prefixes (`"F"` silences all F rules).

!!! tip "Recommended starting point"
    Start with the strictest setup (all defaults). Add `warning_rules` only for rules where your team has a documented reason to tolerate the pattern.

---

## Line suppression

To suppress a rule on one specific line, add a `# noqa: pap:` comment:

```python
result = df.collect()                  # noqa: pap: D001
bad_join = df.crossJoin(other)         # noqa: pap: S010, S002
df = df.withColumn("x", expr("a+b"))   # noqa: pap: F017
```

Multiple rules can be suppressed on the same line by comma-separating them.
