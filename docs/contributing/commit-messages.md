# Commit message convention

Every commit message must start with one of the four prefixes below.
A `commit-msg` git hook enforces this automatically.

!!! tip "First-time setup — activate the commit-msg hook"
    This repository ships a `commit-msg` hook that enforces the commit prefix
    convention (`feat:`, `fix:`, `enhance:`, `breaking:`).
    Run this once after cloning:

    ```bash
    git config core.hooksPath .githooks
    ```

---

## Prefixes

| Prefix | When to use |
|---|---|
| `feat:` | A new rule, CLI flag, configuration option, or user-visible capability |
| `fix:` | A bug fix — incorrect detection, false positive, false negative, crash |
| `enhance:` | An improvement to something that already exists (better error message, performance, refactor) |
| `breaking:` | A change that alters existing behaviour in a way that requires users to update their configuration or workflows |

---

## Format

```
<prefix> <short description>

<optional body>
```

- The **first line** (subject) must start with the prefix, followed by a space
  and a concise description in the imperative mood ("add", "fix", "remove" —
  not "added", "fixes", "removed").
- Keep the subject line under **72 characters**.
- Leave a **blank line** between the subject and the optional body.
- The body is free-form: explain *why* the change was made, not just *what*.

---

## Examples

```
feat: add PERF007 to detect uncached DataFrames in join/union chains
```

```
fix: prevent os.path.join from triggering PERF007

os.path.join shares the method name "join" with the Spark DataFrame API.
Added a NON_DATAFRAME_ROOTS guard in both the pre-scan and the ref-counting
phase so that stdlib path operations are never treated as Spark operations.
```

```
enhance: improve PERF005 violation position to point at persist() method name
```

```
breaking: update configuration keys from select/warn/ignore to new schema

Users must rename the [rules] section keys in their
.pyspark-antipattern.toml files.
```

---

## What is rejected

The hook rejects any commit whose subject does not start with one of the four
prefixes.  Common patterns that will be blocked:

```
update PERF007         ✗  — use enhance: or fix:
WIP                    ✗  — squash before merging
misc fixes             ✗  — be specific, use fix:
refactor rule content  ✗  — use enhance:
```
