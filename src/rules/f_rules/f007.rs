// F007: Prefer filter() before select() — detect select().filter() pattern
use rustpython_parser::ast::{Expr, Stmt};

use crate::{
    config::Config,
    line_index::LineIndex,
    rules::utils::method_violation,
    violation::Violation,
    visitor::{Visitor, walk_expr},
};

const ID: &str = "F007";

struct Check<'a> {
    source: &'a str,
    file: &'a str,
    index: &'a LineIndex,
    severity: crate::violation::Severity,
    violations: Vec<Violation>,
}

impl<'a> Visitor for Check<'a> {
    fn visit_expr(&mut self, expr: &Expr) {
        // Pattern: filter() called on the result of select()
        if let Expr::Call(call) = expr
            && let Expr::Attribute(outer) = call.func.as_ref()
            && (outer.attr.as_str() == "filter" || outer.attr.as_str() == "where")
            && let Expr::Call(inner_call) = outer.value.as_ref()
            && let Expr::Attribute(inner_attr) = inner_call.func.as_ref()
            && inner_attr.attr.as_str() == "select"
        {
            self.violations.push(method_violation(
                outer,
                outer.attr.as_str(),
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
