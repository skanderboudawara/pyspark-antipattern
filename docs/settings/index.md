# Settings

All settings live under `[tool.pyspark-antipattern]` in your `pyproject.toml`.

| Setting | Type | Default | Description |
|---|---|---|---|
| [`select`](select.md) | `list[str]` | `[]` | Show only these rules (whitelist) |
| [`warn`](warn.md) | `list[str]` | `[]` | Rules downgraded to warnings |
| [`ignore`](ignore.md) | `list[str]` | `[]` | Rules completely silenced |
| [`severity`](severity.md) | `str` | `null` | Minimum performance-impact level to report (`"low"`, `"medium"`, `"high"`) |
| [`show_information`](show_information.md) | `bool` | `false` | Show inline explanation per violation |
| [`show_best_practice`](show_best_practice.md) | `bool` | `false` | Show best-practice guidance per violation |
| [`distinct_threshold`](distinct_threshold.md) | `int` | `5` | Max `.distinct()` calls before S004 fires |
| [`explode_threshold`](explode_threshold.md) | `int` | `3` | Max `explode()` calls before S008 fires |
| [`loop_threshold`](loop_threshold.md) | `int` | `10` | Max loop iterations before L001/L002/L003 fire |
| [`exclude_dirs`](exclude_dirs.md) | `list[str]` | built-in list | Directories skipped during recursive scan |
| [`max_shuffle_operations`](max_shuffle_operations.md) | `int` | `9` | Max shuffle ops between checkpoints before PERF003 fires |
