// ARR006: Avoid size(collect_list(...).over(w)) — use count(...).over(w) instead.
//
// size(collect_list(col).over(w)) materialises every value in the window into
// an in-memory array and then measures its length. count(col).over(w) computes
// the same row count directly inside the window without building the array.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "ARR006";

fn is_named(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Name(n) => n.id.as_str() == name,
        Expr::Attribute(a) => a.attr.as_str() == name,
        _ => false,
    }
}

/// Returns `true` when `expr` is `size(collect_list(...).over(...))`.
fn is_size_of_windowed_collect_list(expr: &Expr) -> bool {
    if let Expr::Call(outer) = expr
        && is_named(&outer.func, "size")
        && !outer.args.is_empty()
    {
        // The sole argument must be collect_list(...).over(...)
        if let Expr::Call(over_call) = &outer.args[0]
            && let Expr::Attribute(a) = over_call.func.as_ref()
            && a.attr.as_str() == "over"
            && let Expr::Call(inner) = a.value.as_ref()
        {
            return is_named(&inner.func, "collect_list");
        }
    }
    false
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
        if is_size_of_windowed_collect_list(expr) {
            self.violations.push(expr_violation(
                expr,
                "size".len(),
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
