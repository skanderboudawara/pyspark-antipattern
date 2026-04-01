# Adding a new linting rule

Each rule lives in exactly **7 places**. Follow the steps below in order and
nothing will be missed.

---

## Step 1 — Write the documentation

Create `docs/rules/<category>/RULEXXX.md`.
The file must contain at least an `## Information` section and a
`## Best practices` section — these are parsed at compile-time and shown by the
`--show-information` / `--show-best-practice` CLI flags.

```markdown
# Rule RULEXXX
Short one-line description of what the rule catches

## Information
Explain *why* the pattern is problematic. Use bullet points, code diagrams,
and concrete consequences.

## Best practices
Show a **Bad** snippet and a **Good** snippet with `python` fenced blocks.

### Example

Bad:
\```python
# the antipattern
\```

Good:
\```python
# the fix
\```
```

Then add a row to the category index `docs/rules/<category>/index.md`:

```markdown
| [RULEXXX](RULEXXX.md) | Short description |
```

---

## Step 2 — Implement the rule in Rust

Create `src/rules/<category>_rules/rulexxx.rs`.

Use an existing simple rule as a starting point (e.g. `perf004.rs` for a
method-call check, `perf005.rs` for a two-pass scope analysis).

The mandatory public entry point is:

```rust
pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> { … }
```

Key utilities in `src/rules/utils.rs`:

| Helper | Use it to … |
|---|---|
| `method_violation(attr, name, …)` | point the caret at a method name |
| `expr_violation(expr, span, …)` | point the caret at an expression start |
| `expr_start(expr)` | get the byte offset of any expression |
| `chain_has_method(expr, name)` | check whether a method appears in a call chain |
| `is_non_dataframe_receiver(expr)` | skip stdlib/path receivers (`os`, `sys`, …) |

The `config.severity_of(ID)` call resolves the rule's severity from the user's
`.pyspark-antipattern.toml` (or the default).

---

## Step 3 — Register the module

Open `src/rules/<category>_rules/mod.rs` and add:

```rust
pub mod rulexxx;
```

---

## Step 4 — Add the rule to the global dispatch table

Open `src/rules/mod.rs` and append to the `ALL_RULES` slice:

```rust
<category>_rules::rulexxx::check,
```

---

## Step 5 — Embed the documentation

Open `src/rule_content.rs` and add one line to the `RULE_MARKDOWN` slice:

```rust
("RULEXXX", include_str!("../docs/rules/<category>/RULEXXX.md")),
```

This embeds the markdown at compile-time so the CLI can display it with
`--show-information` and `--show-best-practice`.

---

## Step 6 — Register the rule title in the reporter

Open `src/reporter.rs` and add a match arm to the `rule_title` function:

```rust
"RULEXXX" => "Short one-line description shown in terminal output",
```

Without this entry the terminal output shows `Unknown rule` instead of the
rule title.

---

## Step 7 — Write tests

Open `tests/test_<category>_rules.rs` and add at least:

- One test that **fires** on a minimal bad snippet
- One test that **does not fire** on the corrected snippet

```rust
// ── RULEXXX: short description ───────────────────────────────────────────────
#[test]
fn rulexxx_fires() {
    assert_violation(&check(rulexxx::check, "bad_snippet_here"), "RULEXXX", 1);
}
#[test]
fn rulexxx_no_fire() {
    assert_no_violation(&check(rulexxx::check, "good_snippet_here"), "RULEXXX");
}
```

Run the tests to confirm:

```bash
cargo test rulexxx
```

---

## Checklist

| # | File | Action |
|---|---|---|
| 1 | `docs/rules/<category>/RULEXXX.md` | Create rule documentation |
| 1 | `docs/rules/<category>/index.md` | Add table row |
| 2 | `src/rules/<category>_rules/rulexxx.rs` | Implement `check()` |
| 3 | `src/rules/<category>_rules/mod.rs` | `pub mod rulexxx;` |
| 4 | `src/rules/mod.rs` | Append to `ALL_RULES` |
| 5 | `src/rule_content.rs` | `include_str!` entry |
| 6 | `src/reporter.rs` | `rule_title` match arm |
| 7 | `tests/test_<category>_rules.rs` | Fire + no-fire tests |
