// F015: Avoid consecutive .filter()/.where() calls — combine into one
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "F015";

fn is_filter(name: &str) -> bool {
    matches!(name, "filter" | "where")
}

/// Returns true when the expression is a .filter()/.where() call whose
/// receiver is itself a .filter()/.where() call (consecutive chain of 2+).
fn consecutive_filter_depth(expr: &Expr) -> usize {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref()
            && is_filter(a.attr.as_str()) {
                return 1 + consecutive_filter_depth(a.value.as_ref());
            }
    0
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
    seen: std::collections::HashSet<(usize, usize)>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
                && is_filter(attr.attr.as_str())
                    && consecutive_filter_depth(expr) >= 2
                {
                    let end: u32 = attr.range.end().into();
                    let name = attr.attr.as_str();
                    let start = end.saturating_sub(name.len() as u32);
                    let (line, col) = self.index.line_col(start);
                    if self.seen.insert((line, col)) {
                        self.violations.push(method_violation(
                            attr, name, self.source, self.file,
                            self.index, self.severity, ID,
                        ));
                    }
                }
        walk_expr(self, expr);
    }
}

pub fn check(
    stmts: &[Stmt],
    source: &str,
    file: &str,
    config: &Config,
    index: &LineIndex,
) -> Vec<Violation> {
    let mut v = Check {
        source, file, index,
        severity: config.severity_of(ID),
        violations: vec![],
        seen: std::collections::HashSet::new(),
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
