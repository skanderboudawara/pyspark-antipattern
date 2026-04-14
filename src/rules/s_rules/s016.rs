//! S016: `first()` or `last()` with `.over(Window.partitionBy(...))` without
//! `orderBy()` in the window spec — the result is non-deterministic because
//! partition ordering is undefined without an explicit sort.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::{chain_has_method, method_violation},
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "S016";

fn is_named(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Name(n) => n.id.as_str() == name,
        Expr::Attribute(a) => a.attr.as_str() == name,
        _ => false,
    }
}

/// Returns `true` when `expr` is a call to `first(...)` or `last(...)`,
/// with or without module qualification (e.g. `F.first(...)`).
fn is_first_or_last(expr: &Expr) -> bool {
    if let Expr::Call(call) = expr {
        return is_named(&call.func, "first") || is_named(&call.func, "last");
    }
    false
}

/// Returns `true` when the window-spec expression chain contains `partitionBy`
/// but **not** `orderBy`.
fn is_partition_only_window(expr: &Expr) -> bool {
    chain_has_method(expr, "partitionBy") && !chain_has_method(expr, "orderBy")
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
        // Look for .over(...) calls.
        if let Expr::Call(call) = expr
            && let Expr::Attribute(attr) = call.func.as_ref()
            && attr.attr.as_str() == "over"
            && !call.args.is_empty()
        {
            // The receiver of .over() must be first() or last().
            if is_first_or_last(attr.value.as_ref()) {
                // The first argument to .over() is the window spec.
                let window_spec = &call.args[0];
                if is_partition_only_window(window_spec) {
                    self.violations.push(method_violation(
                        attr,
                        "over",
                        self.source,
                        self.file,
                        self.index,
                        self.severity,
                        ID,
                    ));
                }
            }
        }
        walk_expr(self, expr);
    }
}

/// Scan `stmts` for `first()` / `last()` with `.over(Window.partitionBy(...))`
/// that has no `orderBy()` in the window specification.
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
