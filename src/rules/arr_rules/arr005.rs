//! ARR005: Avoid `size(collect_list(...))` inside `.agg()` — use `count()` instead.
//
// size(collect_list(col)) counts all values (including duplicates) by first
// collecting every value into an in-memory array (a full shuffle), then
// measuring the array length. count(col) performs the same count in a single
// efficient aggregation pass without materialising the intermediate array.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "ARR005";

fn is_named(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Name(n) => n.id.as_str() == name,
        Expr::Attribute(a) => a.attr.as_str() == name,
        _ => false,
    }
}

/// Returns `true` when `expr` is `size(collect_list(...))` (with or without
/// module qualification, e.g. `F.size(F.collect_list(...))`).
fn is_size_of_collect_list(expr: &Expr) -> bool {
    if let Expr::Call(outer) = expr
        && is_named(&outer.func, "size")
        && !outer.args.is_empty()
        && let Expr::Call(inner) = &outer.args[0]
    {
        return is_named(&inner.func, "collect_list");
    }
    false
}

/// Strip a trailing `.alias(...)` call, returning the inner expression.
fn strip_alias(expr: &Expr) -> &Expr {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref()
        && a.attr.as_str() == "alias"
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
        // Only look inside .agg(...) calls.
        if let Expr::Call(call) = expr
            && let Expr::Attribute(a) = call.func.as_ref()
            && a.attr.as_str() == "agg"
        {
            for arg in &call.args {
                let inner = strip_alias(arg);
                if is_size_of_collect_list(inner) {
                    self.violations.push(expr_violation(
                        inner,
                        "size".len(),
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

/// Scan `stmts` for `size(collect_list(...))` inside `.agg()` calls and flag each occurrence.
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
