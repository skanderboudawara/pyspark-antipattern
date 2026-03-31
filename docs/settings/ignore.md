# ignore

**Type:** `list[str]`
**Default:** `[]`

---

## Description

Rules listed here are **completely silenced** — no output, no exit code impact. They are never reported regardless of how many times they would have fired.

Use this for rules that are not relevant to your codebase at all, or for patterns your team has deliberately accepted.

Accepts exact rule IDs or single-letter group prefixes.

---

## Examples

```toml
[tool.pyspark-antipattern]
# Silence one specific rule
ignore = ["S004"]
```

```toml
[tool.pyspark-antipattern]
# Silence an entire category
ignore = ["F"]
```

```toml
[tool.pyspark-antipattern]
# Mix: silence all S and L rules, plus one specific D rule
ignore = ["S", "L", "D001"]
```

---

## Notes

!!! warning
    Ignoring a rule means violations are never surfaced — not even as warnings. Prefer [`warn`](warn.md) if you still want visibility without blocking CI.

- Group prefix `"U"` silences all UDF rules (U001, U002, U003, U004)
- For per-line suppression, use `# noqa: pap: RULE_ID` instead — see [Installation](../installation.md#suppressing-a-specific-line)
