//! F009: Avoid nested `when()` calls — use a flat chain of `.when().when().otherwise()`
//! for improved readability and Catalyst optimizer compatibility.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F009";

fn is_when_call(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr {
        match c.func.as_ref() {
            Expr::Name(n) => return n.id.as_str() == "when",
            Expr::Attribute(a) => return a.attr.as_str() == "when",
            _ => {}
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
        // Detect a `when(...)` call whose arguments contain another `when(...)` call.
        if is_when_call(expr)
            && let Expr::Call(c) = expr
        {
            for arg in &c.args {
                if is_when_call(arg) {
                    self.violations.push(expr_violation(
                        arg,
                        "when()".len(),
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

/// Scan `stmts` for `when(...)` calls whose arguments contain another `when(...)` call.
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
