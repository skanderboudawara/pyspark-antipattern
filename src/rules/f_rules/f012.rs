// F012: Always wrap literal values with lit()
// Detects bare numeric/string/bool constants passed as column value arguments
// to withColumn(), withColumns(), select(), and similar methods.
use rustpython_parser::ast::{Constant, Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{walk_expr, Visitor},
};

const ID: &str = "F012";

fn is_bare_literal(expr: &Expr) -> bool {
    if let Expr::Constant(c) = expr {
        matches!(
            &c.value,
            Constant::Int(_) | Constant::Float(_) | Constant::Str(_) | Constant::Bool(_)
        )
    } else {
        false
    }
}

fn is_lit_wrapped(expr: &Expr) -> bool {
    if let Expr::Call(c) = expr {
        if let Expr::Name(n) = c.func.as_ref() {
            return n.id.as_str() == "lit";
        }
        if let Expr::Attribute(a) = c.func.as_ref() {
            return a.attr.as_str() == "lit";
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

impl<'a> Check<'a> {
    fn check_column_value_arg(&mut self, arg: &Expr) {
        if is_bare_literal(arg) && !is_lit_wrapped(arg) {
            self.violations.push(expr_violation(
                arg, 1, self.source, self.file, self.index, self.severity, ID,
            ));
        }
    }
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        if let Expr::Call(call) = expr {
            if let Expr::Attribute(attr) = call.func.as_ref() {
                match attr.attr.as_str() {
                    "withColumn" => {
                        // withColumn("name", VALUE) — check second argument
                        if call.args.len() >= 2 {
                            self.check_column_value_arg(&call.args[1]);
                        }
                    }
                    "select" | "withColumns" => {
                        // All positional arguments that are bare literals
                        for arg in &call.args {
                            self.check_column_value_arg(arg);
                        }
                    }
                    _ => {}
                }
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
