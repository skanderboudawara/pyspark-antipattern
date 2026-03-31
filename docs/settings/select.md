# select

**Type:** `list[str]`
**Default:** `[]` (all rules are active)

---

## Description

When set, only the listed rules are shown — everything else is silenced. This makes `select` a **whitelist**: violations for rules not in the list produce no output and do not affect the exit code.

Use this when you want to focus a run on a specific rule or category without changing the rest of your configuration.

Accepts exact rule IDs or single-letter group prefixes.

---

## Examples

```toml
[tool.pyspark-antipattern]
# Show only F018 violations — all other rules are silenced
select = ["F018"]
```

```toml
[tool.pyspark-antipattern]
# Focus on all driver-side rules
select = ["D"]
```

```toml
[tool.pyspark-antipattern]
# Focus on two specific rules
select = ["D001", "S002"]
```

---

## Notes

!!! tip
    `select` is most useful for one-off audits or CI jobs that check a single concern.
    For permanent silencing of irrelevant rules, use [`ignore`](ignore.md) instead.

- Group prefix `"F"` selects all rules starting with `F` (F001–F018)
- When `select` is empty (default), all rules run normally
- `select` takes priority over `ignore`: if a rule is in `select`, it will still be silenced if it also appears in `ignore`
