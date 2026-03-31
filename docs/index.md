[![PyPI - Version](https://img.shields.io/pypi/v/pyspark-antipattern)](https://pypi.org/project/pyspark-antipattern/)
[![Release](https://github.com/skanderboudawara/pyspark-antipattern/actions/workflows/release.yml/badge.svg)](https://github.com/skanderboudawara/pyspark-antipattern/actions/workflows/release.yml)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyspark-antipattern)](https://pypi.org/project/pyspark-antipattern/)
[![GitHub Issues or Pull Requests](https://img.shields.io/github/issues/skanderboudawara/pyspark-antipattern)](https://github.com/skanderboudawara/pyspark-antipattern/issues)

# pyspark-antipattern

A fast, opinionated PySpark linter that challenges your code against antipattern rules — written in Rust, installable as a Python package, and designed to run in CI/CD pipelines.

!!! quote "Philosophy"
    This linter is intentionally strict. It will flag patterns that are technically valid Python but known to cause performance, scalability, or maintainability problems in PySpark. Every violation is a conversation starter — it is up to you to decide whether to fix it, downgrade it to a warning, or suppress it for a specific line.

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

---

## Author

**Skander Boudawara** — [skander.education@proton.me](mailto:skander.education@proton.me)
