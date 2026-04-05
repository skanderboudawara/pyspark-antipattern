//! F020: Avoid `select("*")` — use explicit column names instead of the wildcard
//! to make the DataFrame schema a visible contract in code.
use rustpython_parser::ast::{Constant, Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F020";

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
            && attr.attr.as_str() == "select"
        {
            let has_star = call.args.iter().any(|arg| {
                matches!(
                    arg,
                    Expr::Constant(c)
                        if matches!(&c.value, Constant::Str(s) if s == "*")
                )
            });
            if has_star {
                self.violations.push(method_violation(
                    attr,
                    "select",
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

/// Scan `stmts` for `.select("*")` calls and return a violation for each wildcard argument.
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
