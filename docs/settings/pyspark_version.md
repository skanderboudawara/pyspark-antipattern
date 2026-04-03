# pyspark_version

**Type:** `str`
**Default:** `null` (all rules shown regardless of version)
**Format:** `"X.Y"` or `"X.Y.Z"` (e.g. `"3.4"`, `"3.5.1"`)

---

## Description

Tells the linter which PySpark version your cluster runs. Rules that reference
APIs introduced in a newer version are silenced — they are irrelevant if your
cluster cannot use those APIs yet.

Each rule has a `## PySpark version` section in its documentation indicating
the minimum version it applies to. When `pyspark_version` is set, only rules
whose minimum version is **less than or equal to** your configured version are
reported.

---

## Example

```toml
[tool.pyspark-antipattern]
# My cluster runs PySpark 3.4 — suppress rules that require 3.5+
pyspark_version = "3.4"
```

```toml
[tool.pyspark-antipattern]
# Pin to an exact patch release
pyspark_version = "3.5.1"
```

---

## CLI equivalent

```bash
pyspark-antipattern check src/ --pyspark-version=3.4
pyspark-antipattern check src/ --pyspark-version=3.5.1
```

When both `pyproject.toml` and `--pyspark-version` are set, the CLI flag
takes precedence.

---

## Notes

- When `pyspark_version` is **not set** (the default), all rules are shown
  regardless of their minimum version requirement.
- Setting `pyspark_version = "3.0"` is equivalent to the default since all
  current rules apply from PySpark 3.0.
- The version filter is independent of `severity`, `select`, `warn`, and
  `ignore` — all filters are applied together.
