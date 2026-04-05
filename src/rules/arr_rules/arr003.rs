//! ARR003: Avoid `array_distinct(collect_set())` — `collect_set()` already returns distinct values.
//
// collect_set() deduplicates during aggregation (a shuffle).
// Wrapping the result in array_distinct() runs a second deduplication pass
// over data that is already unique, wasting CPU and potentially triggering
// an additional sort.
//
// Covers both:
//   array_distinct(collect_set(...))          — direct nesting
//   array_distinct(collect_set(...).over(w))  — window aggregate variant
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "ARR003";

fn is_named(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Name(n) => n.id.as_str() == name,
        Expr::Attribute(a) => a.attr.as_str() == name,
        _ => false,
    }
}

/// Strips a trailing `.over(...)` window call, returning the inner expression.
fn unwrap_window(expr: &Expr) -> &Expr {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref()
        && a.attr.as_str() == "over"
    {
        return a.value.as_ref();
    }
    expr
}

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(outer) = expr
            && is_named(&outer.func, "array_distinct")
            && let Some(arg) = outer.args.first()
            && let Expr::Call(inner) = unwrap_window(arg)
            && is_named(&inner.func, "collect_set")
        {
            self.violations.push(expr_violation(
                expr,
                "array_distinct".len(),
                self.source,
                self.file,
                self.index,
                self.severity,
                ID,
            ));
        }
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for `array_distinct(collect_set(...))` patterns and flag each redundant dedup.
pub fn check(stmts: &[Stmt], source: &str, file: &str, config: &Config, index: &LineIndex) -> Vec<Violation> {
    let severity = config.severity_of(ID);
    let mut v = Check {
        source,
        file,
        index,
        severity,
        violations: vec![],
    };
    for s in stmts {
        v.visit_stmt(s);
    }
    v.violations
}
