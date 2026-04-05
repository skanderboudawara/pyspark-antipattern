// F011: Avoid backslash line continuation; use parentheses
// This rule works on raw source text, not the AST.
use rustpython_parser::ast::Stmt;

use crate::{
    config::Config,
    line_index::LineIndex,
    violation::{RuleId, Violation},
};

const ID: &str = "F011";

pub fn check(_stmts: &[Stmt], source: &str, file: &str, config: &Config, _index: &LineIndex) -> Vec<Violation> {
    let severity = config.severity_of(ID);
    let mut violations = vec![];

    for (idx, line) in source.lines().enumerate() {
        // A trailing backslash is a line continuation.
        // We avoid flagging lines that are obviously inside a string by
        // requiring the backslash to be the very last character and not
        // immediately preceded by another backslash (escaped backslash).
        let trimmed_end = line.trim_end_matches(' ');
        if trimmed_end.ends_with('\\') {
            let line_no = idx + 1;
            let col = trimmed_end.len(); // 1-based col of the backslash
            violations.push(Violation {
                rule_id: RuleId(ID.to_string()),
                severity,
                impact: crate::violation::Impact::Low,
                file: file.to_string(),
                line: line_no,
                col,
                source_line: line.to_string(),
                span_len: 1,
            });
        }
    }

    violations
}
