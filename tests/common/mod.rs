use rustpython_parser::{Mode, ast::Mod, parse};

use pyspark_antipattern::{config::Config, line_index::LineIndex, rules::RuleFn, violation::Violation};

/// Parse `source` and run `rule_fn` against it using default config.
pub fn check(rule_fn: RuleFn, source: &str) -> Vec<Violation> {
    check_with(rule_fn, source, &Config::default())
}

/// Parse `source` and run `rule_fn` against it using the given config.
pub fn check_with(rule_fn: RuleFn, source: &str, config: &Config) -> Vec<Violation> {
    let parsed = parse(source, Mode::Module, "<test>").expect("parse error in test snippet");
    let stmts = match parsed {
        Mod::Module(m) => m.body,
        _ => vec![],
    };
    let index = LineIndex::new(source);
    rule_fn(&stmts, source, "<test>", config, &index)
}

/// Assert that `violations` contains exactly one entry with `rule_id` at `line`.
#[track_caller]
pub fn assert_violation(violations: &[Violation], rule_id: &str, line: usize) {
    let found = violations.iter().any(|v| v.rule_id.0 == rule_id && v.line == line);
    assert!(
        found,
        "expected {rule_id} at line {line}, got: {:#?}",
        violations
            .iter()
            .map(|v| format!("{}:{}", v.rule_id.0, v.line))
            .collect::<Vec<_>>()
    );
}

/// Assert that no violations with `rule_id` are present.
#[track_caller]
pub fn assert_no_violation(violations: &[Violation], rule_id: &str) {
    let found: Vec<_> = violations.iter().filter(|v| v.rule_id.0 == rule_id).collect();
    assert!(found.is_empty(), "expected no {rule_id}, but found: {found:#?}");
}
