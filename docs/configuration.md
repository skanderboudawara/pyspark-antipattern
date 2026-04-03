# Configuration

Add a `[tool.pyspark-antipattern]` section to your project's `pyproject.toml`.

---

## Full reference

```toml
[tool.pyspark-antipattern]

# Show only these rules — everything else is silenced (default: all rules active)
# select = ["D001", "S"]

# Downgrade these rules from error to warning (exit code stays 0)
warn = ["F008", "F011"]

# Completely silence these rules — no output, no exit code impact
# Accepts exact rule IDs or single-letter group prefixes
ignore = ["S004"]                # silence one rule
# ignore = ["F"]                 # silence all F rules
# ignore = ["S", "L", "D001"]    # silence all S and L rules, plus D001

# Only report violations at or above this performance-impact level (default: all)
# severity = "medium"            # show only MEDIUM and HIGH violations
# severity = "high"              # show only HIGH violations

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

# PERF003: fire when more than N shuffle ops occur without a checkpoint (default: 9)
max_shuffle_operations = 9

# Directories to skip during recursive scanning.
# These are merged with the built-in default list below.
# Default: [".bzr", ".direnv", ".eggs", ".git", ".git-rewrite", ".hg",
#            ".mypy_cache", ".nox", ".pants.d", ".pytype", ".ruff_cache",
#            ".svn", ".tox", ".venv", "__pypackages__", "_build",
#            "buck-out", "dist", "node_modules", "venv"]
# exclude_dirs = ["my_generated_code", "vendor"]
```

---

## Rule severity

Every rule defaults to **error** (exit code 1). You can relax individual rules:

| Option | Effect |
|---|---|
| `select` | Only these rules are shown; everything else is silenced |
| `warn` | Rule fires but exit code stays 0 |
| `ignore` | Rule is completely silenced |

All options accept exact IDs (`"D001"`) or single-letter group prefixes (`"F"` targets all F rules).

!!! tip "Recommended starting point"
    Start with the strictest setup (all defaults). Add `warn` only for rules where your team has a documented reason to tolerate the pattern.

---

## Performance impact filter

Each rule has a static **performance impact** level — `low`, `medium`, or `high` — reflecting how severe the antipattern is at scale. The impact badge is shown in the terminal output next to the rule ID:

```
error[D001][HIGH]: Avoid using collect()
error[F005][LOW]: Avoid stacking multiple withColumn() calls
```

Use `severity` to filter out rules below a given impact level:

```toml
[tool.pyspark-antipattern]
severity = "medium"   # report only MEDIUM and HIGH violations
```

| Value | Reports |
|---|---|
| `"low"` | 🟢 LOW + 🟡 MEDIUM + 🔴 HIGH (same as the default) |
| `"medium"` | 🟡 MEDIUM + 🔴 HIGH |
| `"high"` | 🔴 HIGH only |

This is useful in large codebases where you want to tackle the highest-impact issues first, or in CI pipelines where only critical violations should block a merge.

---

## Line suppression

To suppress a rule on one specific line, add a `# noqa: pap:` comment:

```python
result = df.collect()                  # noqa: pap: D001
bad_join = df.crossJoin(other)         # noqa: pap: S010, S002
df = df.withColumn("x", expr("a+b"))   # noqa: pap: F017
```

Multiple rules can be suppressed on the same line by comma-separating them.
