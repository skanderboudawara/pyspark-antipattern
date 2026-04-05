//! S003: `.groupBy()` directly followed by `.distinct()` or `.dropDuplicates()` —
//! the dedup after an aggregation is redundant since `groupBy` already produces distinct keys.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{chain_has_method, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "S003";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
        {
            let method = attr.attr.as_str();
            if matches!(method, "distinct" | "dropDuplicates") && chain_has_method(attr.value.as_ref(), "groupBy") {
                self.violations.push(method_violation(
                    attr,
                    method,
                    self.source,
                    self.file,
                    self.index,
                    self.severity,
                    ID,
                ));
            }
        }
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for `.groupBy()` calls directly chained with `.distinct()` / `.dropDuplicates()`.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let mut v = Check {
        source,
        file,
        index,
        severity: config.severity_of(ID),
        violations: vec![],
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
