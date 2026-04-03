# Adding a new linting rule

Each rule lives in exactly **9 places**. Follow the steps below in order and
nothing will be missed.

---

## Step 1 — Write the documentation

Create `docs/rules/<category>/RULEXXX.md`.
The file must contain at least a `## Severity` section, an `## Information`
section, and a `## Best practices` section. The severity section is displayed
on the documentation site and must match the entry added in Step 6.

```markdown
# Rule RULEXXX
Short one-line description of what the rule catches

## Severity

🟢 **LOW** — Minor performance impact.

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

Use the appropriate badge for the rule's impact level:

| Impact | Badge |
|---|---|
| Low | `🟢 **LOW** — Minor performance impact.` |
| Medium | `🟡 **MEDIUM** — Moderate performance impact.` |
| High | `🔴 **HIGH** — Major performance impact.` |

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

## Step 6 — Register the rule title and severity in the reporter

Open `src/reporter.rs` and add a match arm to **both** functions:

**`rule_title`** — shown in the terminal next to the rule ID:
```rust
"RULEXXX" => "Short one-line description shown in terminal output",
```

**`rule_impact`** — controls the colored `[LOW]` / `[MEDIUM]` / `[HIGH]` badge
in terminal output and the `--severity` filter. Add the rule ID to the
correct impact arm:
```rust
// pick the right arm: Impact::Low, Impact::Medium, or Impact::High
"RULEXXX" | … => Impact::High,
```

The impact level here must match the `## Severity` badge in the rule's
markdown documentation.

---

## Step 8 — Add the rule to the MkDocs navigation

Open `mkdocs.yml` and add the new page under the correct category section in
the `nav` tree:

```yaml
- RULEXXX: rules/<category>/RULEXXX.md
```

Without this entry the documentation page is built but unreachable from the
site navigation.

---

## Step 9 — Write tests

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
| 1 | `docs/rules/<category>/RULEXXX.md` | Create rule documentation (with `## Severity` badge) |
| 1 | `docs/rules/<category>/index.md` | Add table row |
| 2 | `src/rules/<category>_rules/rulexxx.rs` | Implement `check()` |
| 3 | `src/rules/<category>_rules/mod.rs` | `pub mod rulexxx;` |
| 4 | `src/rules/mod.rs` | Append to `ALL_RULES` |
| 5 | `src/rule_content.rs` | `include_str!` entry |
| 6 | `src/reporter.rs` | `rule_title` match arm + `rule_impact` match arm |
| 8 | `mkdocs.yml` | Add nav entry |
| 9 | `tests/test_<category>_rules.rs` | Fire + no-fire tests |
