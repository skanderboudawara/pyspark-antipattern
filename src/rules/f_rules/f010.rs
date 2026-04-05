// F010: Always include otherwise() at the end of a when() chain
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "F010";

/// Returns true if this expression is a when/otherwise chain (i.e. starts with `when()`).
fn is_when_chain(expr: &Expr) -> bool {
    match expr {
        Expr::Call(c) => match c.func.as_ref() {
            Expr::Name(n) => n.id.as_str() == "when",
            Expr::Attribute(a) => {
                if a.attr.as_str() == "when" || a.attr.as_str() == "otherwise" {
                    is_when_chain(a.value.as_ref())
                } else {
                    false
                }
            }
            _ => false,
        },
        _ => false,
    }
}

/// Returns true if the topmost call in this chain is `.otherwise(...)`.
fn top_is_otherwise(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr
        && let Expr::Attribute(a) = c.func.as_ref() {
            return a.attr.as_str() == "otherwise";
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

impl<'a> Check<'a> {
    fn inspect_arg(&mut self, arg: &Expr) {
        if is_when_chain(arg) && !top_is_otherwise(arg) {
            self.violations.push(expr_violation(
                arg, "when(...)".len(), self.source, self.file, self.index,
                self.severity, ID,
            ));
        }
    }
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        // Check arguments to calls and values in assignments
        if let Expr::Call(c) = expr {
            for arg in &c.args {
                self.inspect_arg(arg);
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
    };
    for s in stmts { v.visit_stmt(s); }
    v.violations
}
