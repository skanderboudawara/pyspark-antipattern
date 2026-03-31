# CLI reference

## Synopsis

```
pyspark-antipattern check <PATH> [OPTIONS]
```

`<PATH>` can be a single `.py` file or a directory (scanned recursively).

---

## Options

All options are optional.  When both a `pyproject.toml` section and a CLI flag
are present, **the CLI flag wins**.

### `--config`

```
--config <FILE>        default: pyproject.toml
```

Path to the `pyproject.toml` that contains the `[tool.pyspark-antipattern]`
section.  Useful in monorepos where the config lives outside the working
directory.

```bash
pyspark-antipattern check src/ --config infra/pyproject.toml
```

---

### `--select`

```
--select=<ID,...>
```

Show only the listed rules — everything else is silenced.  Accepts exact rule
IDs or single-letter group prefixes.

```bash
# Show only F018
pyspark-antipattern check src/ --select=F018

# Show only driver-side rules
pyspark-antipattern check src/ --select=D

# Show a specific mix
pyspark-antipattern check src/ --select=D001,S002,F018
```

---

### `--warn`

```
--warn=<ID,...>
```

Downgrade rules from **error** to **warning**.  Warnings are printed but do not
cause exit code 1.

```bash
pyspark-antipattern check src/ --warn=F008,F011
```

---

### `--ignore`

```
--ignore=<ID,...>
```

Completely silence one or more rules.  Accepts exact rule IDs or single-letter
group prefixes.  Violations for silenced rules produce no output and do not
affect the exit code.

```bash
# Silence one rule
pyspark-antipattern check src/ --ignore=D001

# Silence an entire category
pyspark-antipattern check src/ --ignore=F

# Silence a mix
pyspark-antipattern check src/ --ignore=S,D001,L003
```

---

### `--show_best_practice`

```
--show_best_practice=<true|false>        default: false
```

Print the *Best practices* section from the rule documentation below each
violation.

```bash
pyspark-antipattern check src/ --show_best_practice=true
```

---

### `--show_information`

```
--show_information=<true|false>        default: false
```

Print the *Information* section from the rule documentation below each
violation.

```bash
pyspark-antipattern check src/ --show_information=true
```

---

### `--distinct_threshold`

```
--distinct_threshold=<N>        default: 5
```

S004 fires when the weighted count of `.distinct()` calls in a file exceeds
this value.  Loop-multiplied calls count more than once.

```bash
pyspark-antipattern check src/ --distinct_threshold=3
```

---

### `--explode_threshold`

```
--explode_threshold=<N>        default: 3
```

S008 fires when the weighted count of `explode()` / `explode_outer()` calls in
a file exceeds this value.

```bash
pyspark-antipattern check src/ --explode_threshold=2
```

---

### `--loop_threshold`

```
--loop_threshold=<N>        default: 10
```

L001/L002/L003 fire when a `for` loop over `range(N)` exceeds this iteration
count.  `while` loops always assume 99 iterations.

```bash
pyspark-antipattern check src/ --loop_threshold=5
```

---

### `--max_shuffle_operations`

```
--max_shuffle_operations=<N>        default: 9
```

PERF003 fires when more than N shuffle-inducing operations occur between two
checkpoints (or between the start of the file and the first checkpoint).

```bash
pyspark-antipattern check src/ --max_shuffle_operations=5
```

---

### `--exclude_dirs`

```
--exclude_dirs=<DIR,...>
```

Directory names to skip during recursive scanning.  Replaces (does not extend)
the built-in default exclusion list.

```bash
pyspark-antipattern check src/ --exclude_dirs=vendor,generated,migrations
```

---

## Combining options

All options can be combined freely:

```bash
pyspark-antipattern check src/pipelines/ \
  --config pyproject.toml \
  --ignore=F008,F011 \
  --warn=S004,S008 \
  --show_best_practice=true \
  --max_shuffle_operations=5 \
  --distinct_threshold=3 \
  --exclude_dirs=tests,vendor
```

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | No error-level violations found |
| `1` | One or more error-level violations found |

Warnings never cause a non-zero exit code.

---

## Priority: CLI vs pyproject.toml

When the same option is set in both places, the CLI flag always takes
precedence.  This makes it easy to tighten or relax rules for a single run
without editing config files — useful in CI matrix builds or one-off audits.

```bash
# pyproject.toml has warn = ["D001"]
# but this run shows only F018:
pyspark-antipattern check src/ --select=F018
```

---

## Getting help

```bash
pyspark-antipattern --help
pyspark-antipattern check --help
```
