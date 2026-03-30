# pyspark-antipattern

A fast, opinionated PySpark linter that challenges your code against antipattern rules — written in Rust, installable as a Python package, and designed to run in CI/CD pipelines.

> **This linter is intentionally strict.** It will flag patterns that are technically valid Python but known to cause performance, scalability, or maintainability problems in PySpark. Every violation is a conversation starter, not necessarily a hard blocker — it is up to you to decide whether to fix it, downgrade it to a warning, or suppress it for a specific line. The goal is to make the trade-offs visible before they become production incidents.

---

## Why this exists

PySpark is easy to misuse. `.collect()` on a 10 GB DataFrame, `.withColumn()` called in a loop, UDFs where built-in functions exist — these patterns work fine locally and silently destroy performance at scale. This tool catches them early, at commit time, before they reach your cluster.

---

## Installation

```bash
pip install pyspark-antipattern
```

---

## Usage

Check a single file:
```bash
pyspark-antipattern check pipeline.py
```

Check an entire directory recursively:
```bash
pyspark-antipattern check src/
```

Use a custom config location:
```bash
pyspark-antipattern check src/ --config path/to/pyproject.toml
```

**Exit codes**
- `0` — no errors (warnings are allowed)
- `1` — one or more error-level violations found

---

## Rules

Rules are organized by category in the [`rules/`](rules/) folder. Each rule has its own markdown file with a full explanation and best-practice guidance.

| Category | Folder | Focus |
|---|---|---|
| **D** — Driver | [`rules/driver/`](rules/driver/) | Actions that pull data to the driver node |
| **F** — Format | [`rules/format/`](rules/format/) | Code style and DataFrame API misuse |
| **L** — Looping | [`rules/looping/`](rules/looping/) | DataFrame operations inside loops |
| **P** — Pandas | [`rules/pandas/`](rules/pandas/) | Pandas interop pitfalls |
| **S** — Shuffle | [`rules/shuffle/`](rules/shuffle/) | Joins, partitioning, and data movement |
| **U** — UDF | [`rules/udf/`](rules/udf/) | User-defined functions and their alternatives |

---

## Configuration

Add a `[tool.pyspark-antipattern]` section to your project's `pyproject.toml`:

```toml
[tool.pyspark-antipattern]

# Rules listed here cause exit code 1 (default: all rules are failing)
# failing_rules = []

# Downgrade these rules from error to warning (exit code stays 0)
warning_rules = ["F008", "F011"]

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

### Suppressing a specific line

Add a `# noqa: pap: RULE_ID` comment to suppress one or more rules on that line:

```python
result = df.collect()  # noqa: pap: D001
bad_join = df.crossJoin(other)  # noqa: pap: S010, S002
```

---

## CI/CD integration

### GitHub Actions

```yaml
- name: Lint PySpark code
  run: |
    pip install pyspark-antipattern
    pyspark-antipattern check src/
```

The job fails automatically if any error-level rule fires. Warnings are reported but do not block the pipeline.

### Pre-commit hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: pyspark-antipattern
        name: PySpark antipattern linter
        entry: pyspark-antipattern check
        language: system
        types: [python]
        pass_filenames: false
        args: ["src/"]
```

---

## A word on strictness

This linter will challenge code that your team may have written deliberately and knowingly. That is by design.

Each violation is not a verdict — it is a question: *"Did you mean to do this, and do you understand the trade-off?"* If the answer is yes, suppress the rule on that line or downgrade it to a warning in your config. If the answer is no, you just avoided a production issue.

The strictest setup is the default: every rule is a hard error. Relax only what you have a documented reason to relax.

---

## License

MIT — see [LICENSE](LICENSE)
