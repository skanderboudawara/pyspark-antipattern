# exclude_dirs

**Type:** `list[str]`
**Default:** see below

---

## Description

A list of directory names to **skip entirely** during recursive scanning. When `pyspark-antipattern check src/` encounters a directory whose name is in this list, it does not descend into it — none of the `.py` files inside are checked.

Matching is done on the **directory name only**, not the full path. A single entry `"vendor"` skips every directory named `vendor` at any depth.

---

## Default value

When `exclude_dirs` is not set, the following directories are excluded automatically:

```
.bzr        .direnv     .eggs       .git        .git-rewrite
.hg         .mypy_cache .nox        .pants.d    .pytype
.ruff_cache .svn        .tox        .venv       __pypackages__
_build      buck-out    dist        node_modules venv
```

---

## Example

```toml
[tool.pyspark-antipattern]
# Replace the default list entirely
exclude_dirs = [
    ".venv", "dist", "node_modules",  # keep some defaults
    "generated",                       # project-specific
    "vendor",
]
```

---

## Notes

!!! warning
    Setting `exclude_dirs` **replaces** the default list entirely. If you want to keep the defaults and add more, copy the default list into your config and append your entries.

- Directory exclusion is applied before any file is read — it is a performance optimisation, not just a filter
- To re-enable scanning inside a normally excluded directory, simply omit it from your `exclude_dirs` list
