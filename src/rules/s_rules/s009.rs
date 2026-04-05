//! S009: Prefer `mapPartitions()` over `map()` for row-level transforms — batching
//! operations per partition avoids repeated Python overhead per row.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "S009";

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
            // Only flag .map() called on something that has .rdd in the chain
            // (to reduce false positives on non-RDD .map() calls).
            if attr.attr.as_str() == "map"
                && (crate::rules::utils::chain_has_method(attr.value.as_ref(), "rdd")
                    || matches!(attr.value.as_ref(), Expr::Attribute(a) if a.attr.as_str() == "rdd"))
            {
                self.violations.push(method_violation(
                    attr,
                    "map",
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

/// Scan `stmts` for `.map()` calls on RDDs and suggest `mapPartitions()` for each.
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
