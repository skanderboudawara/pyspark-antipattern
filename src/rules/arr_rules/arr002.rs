// ARR002: Avoid array_except(col, None/lit(None)) — use array_compact() instead.
use rustpython_parser::ast::{Constant, Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::expr_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "ARR002";

fn is_named(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Name(n) => n.id.as_str() == name,
        Expr::Attribute(a) => a.attr.as_str() == name,
        _ => false,
    }
}

/// Returns true if expr is `None` (Python keyword) or `lit(None)`.
fn is_none_expr(expr: &Expr) -> bool {
    // bare None
    if let Expr::Constant(c) = expr
        && matches!(c.value, Constant::None)
    {
        return true;
    }
    // lit(None)
    if let Expr::Call(c) = expr
        && is_named(&c.func, "lit")
        && let Some(arg) = c.args.first()
        && let Expr::Constant(ac) = arg
        && matches!(ac.value, Constant::None)
    {
        return true;
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
        if let Expr::Call(c) = expr
            && is_named(&c.func, "array_except")
            && c.args.len() >= 2
            && is_none_expr(&c.args[1])
        {
            self.violations.push(expr_violation(
                expr,
                "array_except".len(),
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
