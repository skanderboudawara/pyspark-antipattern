[![PyPI - Version](https://img.shields.io/pypi/v/pyspark-antipattern?cacheSeconds=0)](https://pypi.org/project/pyspark-antipattern/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/pyspark-antipattern)](https://pypi.org/project/pyspark-antipattern/)
[![Release](https://github.com/skanderboudawara/pyspark-antipattern/actions/workflows/release.yml/badge.svg?cacheSeconds=0)](https://github.com/skanderboudawara/pyspark-antipattern/actions/workflows/release.yml)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyspark-antipattern?cacheSeconds=0)](https://pypi.org/project/pyspark-antipattern/)
[![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/skanderboudawara/pyspark-antipattern?cacheSeconds=0)](https://github.com/skanderboudawara/pyspark-antipattern/issues)
[![GitHub Stars](https://img.shields.io/github/stars/skanderboudawara/pyspark-antipattern?style=social)](https://github.com/skanderboudawara/pyspark-antipattern/stargazers)

# pyspark-antipattern

**A static analysis linter for PySpark — catch performance antipatterns before they reach your cluster.**

Written in Rust, installable as a Python package, and designed to run in CI/CD pipelines. 67 rules across 8 categories covering driver actions, shuffle explosions, UDFs, loops, and more.

![demo.gif](https://s13.gifyu.com/images/bqMHC.gif)

!!! quote "Philosophy"
    This linter is intentionally strict. It will flag patterns that are technically valid Python but known to cause performance, scalability, or maintainability problems in PySpark. Every violation is a conversation starter — it is up to you to decide whether to fix it, downgrade it to a warning, or suppress it for a specific line.

---

## What it catches

Real antipatterns, caught at commit time:

| Code | Rule | Why it matters |
|---|---|---|
| `df.collect()` | D001 | Pulls all data to driver — OOM risk on large datasets |
| `for c in cols: df.withColumn(...)` | L003 | Each call adds a projection — plan explodes exponentially |
| `array_distinct(collect_list(x))` | ARR001 | Use `collect_set(x)` — one step instead of two |
| `df.rdd.collect()` | PERF001 | Use `.toPandas()` — 10x faster with Arrow enabled |
| `df.join(other)` | S011 | No condition = Cartesian product |
| `@udf` returning `StringType` | U001 | Built-in string functions are orders of magnitude faster |

---

## Quick start

```bash
pip install pyspark-antipattern
pyspark-antipattern check src/
```

Exit code `0` — no errors. Exit code `1` — one or more error-level violations.

---

## Why this exists

PySpark is easy to misuse. `.collect()` on a 10 GB DataFrame, `.withColumn()` called in a loop, UDFs where built-in functions exist — these patterns work fine locally and silently destroy performance at scale. This tool catches them early, at commit time, before they reach your cluster.

---

## Rules at a glance

| Category | Prefix | Focus |
|---|---|---|
| Array | `ARR` | Array function antipatterns |
| Driver | `D` | Actions that pull data to the driver node |
| Format | `F` | Code style and DataFrame API misuse |
| Looping | `L` | DataFrame operations inside loops |
| Pandas | `P` | Pandas interop pitfalls |
| Performance | `PERF` | Runtime performance antipatterns |
| Shuffle | `S` | Joins, partitioning, and data movement |
| UDF | `U` | User-defined functions and their alternatives |

[Browse all rules →](rules/index.md)

Every rule carries a **severity** badge indicating its performance impact:

| Severity | Meaning |
|---|---|
| 🔴 **HIGH** | Major performance impact — OOM risk, full scans, shuffle explosion |
| 🟡 **MEDIUM** | Moderate performance impact — avoidable overhead at scale |
| 🟢 **LOW** | Minor impact — style, API correctness, small inefficiencies |

Use `--severity` to filter by impact level:

```bash
pyspark-antipattern check src/ --severity=high    # only HIGH violations
pyspark-antipattern check src/ --severity=medium  # MEDIUM and HIGH
```

Or set it permanently in `pyproject.toml`:

```toml
[tool.pyspark-antipattern]
severity = "medium"
```

---

## PySpark version awareness

Each rule knows the minimum PySpark version it applies to. Set `pyspark_version`
to your cluster version and rules referencing newer APIs are silenced automatically:

```bash
pyspark-antipattern check src/ --pyspark-version=3.3
```

```toml
[tool.pyspark-antipattern]
pyspark_version = "3.3"   # suppress rules requiring PySpark 3.4+
```

---

## Author

**Skander Boudawara** — [skander.education@proton.me](mailto:skander.education@proton.me)
