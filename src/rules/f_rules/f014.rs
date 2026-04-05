//! F014: Avoid `explode_outer()` — handle nulls upstream or with higher-order
//! functions rather than expanding null arrays into `null` rows.
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F014";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            let is_explode_outer = match call.func.as_ref() {
                // explode_outer(col("x"))
                Expr::Name(n) => n.id.as_str() == "explode_outer",
                // functions.explode_outer(col("x"))
                Expr::Attribute(a) => a.attr.as_str() == "explode_outer",
                _ => false,
            };
            if is_explode_outer {
                self.violations.push(expr_violation(
                    expr,
                    "explode_outer".len(),
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

/// Scan `stmts` for `explode_outer(...)` calls and return a violation for each one found.
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
