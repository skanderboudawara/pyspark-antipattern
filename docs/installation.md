# Installation

## pip

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

| Code | Meaning |
|---|---|
| `0` | No errors (warnings are allowed) |
| `1` | One or more error-level violations found |

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

## Suppressing a specific line

Add a `# noqa: pap: RULE_ID` comment to suppress one or more rules on that line:

```python
result = df.collect()  # noqa: pap: D001
bad_join = df.crossJoin(other)  # noqa: pap: S010, S002
```
