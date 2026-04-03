#!/usr/bin/env python3
"""
check_rule_consistency.py
=========================
Verify that every linting rule documented in docs/rules/ is fully registered
in all 10 required locations (see docs/contributing/adding-a-rule.md).

Usage:
    python scripts/check_rule_consistency.py

Exit code:
    0  — all rules are complete
    1  — one or more rules are missing an entry somewhere
"""

import sys
from pathlib import Path

# ── Project root (one level above this script) ────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

# ── Category folder → (rust module folder, rule-id prefix) ───────────────────
# The rule-id prefix is used only for sanity; the mapping drives file paths.
CATEGORIES = {
    "arr":         "arr_rules",
    "driver":      "d_rules",
    "format":      "f_rules",
    "looping":     "l_rules",
    "pandas":      "p_rules",
    "performance": "perf_rules",
    "shuffle":     "s_rules",
    "udf":         "u_rules",
}


# ── Discovery ─────────────────────────────────────────────────────────────────

def discover_rules():
    """Return [(rule_id, doc_category)] for every rule found under docs/rules/."""
    rules = []
    docs_rules = ROOT / "docs" / "rules"
    for cat_dir in sorted(docs_rules.iterdir()):
        if not cat_dir.is_dir() or cat_dir.name not in CATEGORIES:
            continue
        for md_file in sorted(cat_dir.glob("*.md")):
            if md_file.name == "index.md":
                continue
            rules.append((md_file.stem.upper(), cat_dir.name))
    return rules


# ── Per-rule checks ───────────────────────────────────────────────────────────

def check_rule(rule_id, doc_category):
    """Return a list of human-readable failure strings for this rule."""
    failures = []
    rust_folder = CATEGORIES[doc_category]
    rule_lower  = rule_id.lower()

    def read(path):
        try:
            return path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return ""

    # 1a ── docs/rules/<category>/RULEXXX.md — ## Severity + ## PySpark version ──
    rule_md = ROOT / "docs" / "rules" / doc_category / f"{rule_id}.md"
    rule_md_text = read(rule_md)
    if "## Severity" not in rule_md_text:
        failures.append(
            f"  docs/rules/{doc_category}/{rule_id}.md"
            f"  — missing `## Severity` section"
        )
    if "## PySpark version" not in rule_md_text:
        failures.append(
            f"  docs/rules/{doc_category}/{rule_id}.md"
            f"  — missing `## PySpark version` section"
        )

    # 1b ── docs/rules/<category>/index.md ────────────────────────────────────
    index_md = ROOT / "docs" / "rules" / doc_category / "index.md"
    if rule_id not in read(index_md):  # noqa: separate file from rule_md above
        failures.append(
            f"  docs/rules/{doc_category}/index.md"
            f"  — missing [{rule_id}] entry"
        )

    # 2 ── src/rules/<rust_folder>/<ruleid>.rs ────────────────────────────────
    rs_file = ROOT / "src" / "rules" / rust_folder / f"{rule_lower}.rs"
    if not rs_file.exists():
        failures.append(
            f"  src/rules/{rust_folder}/{rule_lower}.rs"
            f"  — file does not exist"
        )

    # 3 ── src/rules/<rust_folder>/mod.rs ─────────────────────────────────────
    cat_mod = ROOT / "src" / "rules" / rust_folder / "mod.rs"
    if f"pub mod {rule_lower};" not in read(cat_mod):
        failures.append(
            f"  src/rules/{rust_folder}/mod.rs"
            f"  — missing `pub mod {rule_lower};`"
        )

    # 4 ── src/rules/mod.rs (ALL_RULES) ───────────────────────────────────────
    all_rules = ROOT / "src" / "rules" / "mod.rs"
    entry = f"{rust_folder}::{rule_lower}::check"
    if entry not in read(all_rules):
        failures.append(
            f"  src/rules/mod.rs"
            f"  — missing `{entry}` in ALL_RULES"
        )

    # 5 ── src/rule_content.rs ────────────────────────────────────────────────
    rule_content = ROOT / "src" / "rule_content.rs"
    if f'("{rule_id}",' not in read(rule_content):
        failures.append(
            f"  src/rule_content.rs"
            f"  — missing (\"{rule_id}\", include_str!(...))"
        )

    # 6 ── src/reporter.rs — rule_title() and rule_impact() ──────────────────
    reporter = ROOT / "src" / "reporter.rs"
    reporter_text = read(reporter)
    if f'"{rule_id}" =>' not in reporter_text:
        failures.append(
            f"  src/reporter.rs"
            f"  — missing \"{rule_id}\" => ... in rule_title()"
        )
    if reporter_text.count(f'"{rule_id}"') < 2:
        failures.append(
            f"  src/reporter.rs"
            f"  — missing \"{rule_id}\" in rule_impact()"
        )

    # 7 ── mkdocs.yml ──────────────────────────────────────────────────────────
    mkdocs = ROOT / "mkdocs.yml"
    if f"rules/{doc_category}/{rule_id}.md" not in read(mkdocs):
        failures.append(
            f"  mkdocs.yml"
            f"  — missing `rules/{doc_category}/{rule_id}.md` nav entry"
        )

    return failures


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    rules = discover_rules()
    if not rules:
        print("No rules found under docs/rules/ — check the project structure.")
        sys.exit(1)

    all_failures = {}
    for rule_id, doc_category in rules:
        failures = check_rule(rule_id, doc_category)
        if failures:
            all_failures[rule_id] = failures

    total = len(rules)
    if not all_failures:
        print(f"OK  All {total} rules are fully registered across all 10 locations.")
        sys.exit(0)

    bad = len(all_failures)
    print(f"FAIL  {bad}/{total} rule(s) have incomplete registrations:\n")
    for rule_id in sorted(all_failures):
        print(f"  [{rule_id}]")
        for msg in all_failures[rule_id]:
            print(msg)
        print()
    sys.exit(1)


if __name__ == "__main__":
    main()
